import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from os import cpu_count
from typing import AsyncGenerator, Generator

import fitz
import numpy as np
from toollib.kvalue import KValue

from docsloader.base import BaseLoader, DocsData
from docsloader.utils import format_image

logger = logging.getLogger(__name__)


class PdfLoader(BaseLoader):

    async def load_by_basic(self) -> AsyncGenerator[DocsData, None]:
        pdf_dpi = self.load_options.get("pdf_dpi")
        image_fmt = self.load_options.get("image_fmt")
        max_workers = self.load_options.get("max_workers")
        for item in self.extract_by_pymupdf(
                filepath=self.tmpfile,
                dpi=pdf_dpi,
                image_fmt=image_fmt,
                max_workers=max_workers,
        ):
            self.metadata.update({
                "page": item.get("page"),
                "page_total": item.get("page_total"),
                "page_path": item.get("page_path"),
            })
            yield DocsData(
                type=item.get("type"),
                text=item.get("text"),
                data=item.get("data"),
                metadata=self.metadata,
            )

    def extract_by_pymupdf(
            self,
            filepath: str,
            dpi: int = 300,
            image_fmt: str = "path",
            max_workers: int = None,
    ) -> Generator[dict, None, None]:
        tmp_dir = self.mk_tmpdir()
        if max_workers == 0:
            with fitz.open(filepath) as doc:
                page_total = len(doc)
                for page_idx in range(page_total):
                    for item in self._process_page(
                            doc=doc,
                            page_idx=page_idx,
                            page_total=page_total,
                            tmp_dir=tmp_dir,
                            dpi=dpi,
                            image_fmt=image_fmt,
                    ):
                        yield item
                return
        kv = KValue()
        max_workers = max_workers or cpu_count()
        with fitz.open(filepath) as doc:
            page_total = len(doc)
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._process_and_save_page, **{
                    "filepath": filepath,
                    "page_idx": page_idx,
                    "page_total": page_total,
                    "tmp_dir": tmp_dir,
                    "dpi": dpi,
                    "image_fmt": image_fmt,
                    "kvfile": kv.file,
                })
                for page_idx in range(page_total)
            ]
            for future in as_completed(futures):
                try:
                    for key in future.result() or []:
                        yield kv.get(key)
                except Exception as e:
                    logger.error(f"Task failed: {e}")
        kv.remove()
        self.rm_empty_dir(tmp_dir)

    def _process_and_save_page(
            self,
            filepath: str,
            page_idx: int,
            page_total: int,
            tmp_dir: str,
            dpi: int,
            image_fmt: str,
            kvfile: str,
    ) -> list:
        kv = KValue(file=kvfile)
        with fitz.open(filepath) as doc:
            data, idx = [], 0
            for item in self._process_page(
                    doc=doc,
                    page_idx=page_idx,
                    page_total=page_total,
                    tmp_dir=tmp_dir,
                    dpi=dpi,
                    image_fmt=image_fmt,
            ):
                key = f"{page_idx}.{idx}"
                kv.set(key, item)
                data.append(key)
                idx += 1
            return data

    def _process_page(
            self,
            doc,
            page_idx: int,
            page_total: int,
            tmp_dir: str,
            dpi: int,
            image_fmt: str,
    ) -> Generator[dict, None, None]:
        page = doc.load_page(page_idx)
        page_pix = page.get_pixmap(dpi=dpi, alpha=False)
        ext = "png" if page_pix.alpha else "jpg"
        page_path = os.path.join(tmp_dir, f"image_{page_idx}.{ext}")
        try:
            page_pix.save(page_path)
        except Exception as e:
            page_path = None
            logger.error(f"Failed to save image: {e}")
        finally:
            if 'page_pix' in locals():
                del page_pix
        if self._is_two_column(page):
            page_text = self._extract_adaptive_columns(page)
        else:
            page_text = page.get_text("text")
        if page_text.strip():
            yield {
                "type": "text",
                "text": page_text,
                "page": page_idx + 1,
                "page_total": page_total,
                "page_path": page_path,
            }
        # image
        for img_idx, img in enumerate(page.get_images(full=True)):
            xref = img[0]
            pix = fitz.Pixmap(doc, xref)
            if pix.colorspace not in (fitz.csGRAY, fitz.csRGB, fitz.csCMYK):
                pix = fitz.Pixmap(fitz.csRGB, pix)
                ext = "png"
            else:
                ext = "png" if pix.alpha else "jpg"
            image_path = os.path.join(tmp_dir, f"image_{page_idx}-{img_idx}.{ext}")
            try:
                pix.save(image_path)
                yield {
                    "type": "image",
                    "text": format_image(image_path, fmt=image_fmt),  # noqa
                    "data": image_path,
                    "page": page_idx + 1,
                    "page_total": page_total,
                    "page_path": page_path,
                }
            except Exception as e:
                logger.error(f"Failed to save image: {e}")
            finally:
                if 'pix' in locals():
                    del pix

    @staticmethod
    def _is_two_column(page, margin_threshold=0.1) -> bool:
        blocks = page.get_text("blocks")
        if not blocks:
            return False
        x_centers = []
        for b in blocks:
            if b[4].strip():  # 忽略空白块
                x_center = (b[0] + b[2]) / 2
                x_centers.append(x_center)
        if len(x_centers) < 2:
            return False
        hist, bin_edges = np.histogram(x_centers, bins=10)
        peaks = np.where(hist > len(x_centers) * 0.2)[0]
        if len(peaks) == 2 and (bin_edges[peaks[1]] - bin_edges[peaks[0] + 1]) > page.rect.width * margin_threshold:
            return True
        return False

    @staticmethod
    def _extract_adaptive_columns(page) -> str:
        text_blocks = [b for b in page.get_text("blocks") if b[4].strip()]
        if not text_blocks:
            return ""
        x_coords = sorted([(b[0] + b[2]) / 2 for b in text_blocks])
        gaps = [x_coords[i + 1] - x_coords[i] for i in range(len(x_coords) - 1)]
        max_gap_index = np.argmax(gaps)
        split_x = (x_coords[max_gap_index] + x_coords[max_gap_index + 1]) / 2
        left_col, right_col = [], []
        for b in sorted(text_blocks, key=lambda x: (-x[1], x[0])):
            block_center = (b[0] + b[2]) / 2
            if block_center < split_x:
                left_col.append(b[4])
            else:
                right_col.append(b[4])
        return "\n".join(left_col + right_col)
