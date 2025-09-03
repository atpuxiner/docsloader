import logging
import os
from concurrent.futures import ProcessPoolExecutor, as_completed
from os import cpu_count
from typing import AsyncGenerator, Generator

import fitz
import numpy as np
import pdfplumber
from toollib.kvalue import KValue

from docsloader.base import BaseLoader, DocsData
from docsloader.utils import format_image, format_table

logger = logging.getLogger(__name__)


class PdfLoader(BaseLoader):

    async def load_by_basic(self) -> AsyncGenerator[DocsData, None]:
        pdf_keep_page_image = self.load_options.get("pdf_keep_page_image")
        pdf_keep_emdb_image = self.load_options.get("pdf_keep_emdb_image")
        pdf_dpi = self.load_options.get("pdf_dpi")
        max_workers = self.load_options.get("max_workers")
        image_fmt = self.load_options.get("image_fmt")
        for item in self.extract_by_pymupdf(
                filepath=self.tmpfile,
                keep_page_image=pdf_keep_page_image,
                keep_emdb_image=pdf_keep_emdb_image,
                dpi=pdf_dpi,
                max_workers=max_workers,
                image_fmt=image_fmt,
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

    async def load_by_pdfplumber(self) -> AsyncGenerator[DocsData, None]:
        for item in self.extract_by_pdfplumber(filepath=self.tmpfile):
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
            keep_page_image: bool = False,
            keep_emdb_image: bool = False,
            dpi: int = 300,
            max_workers: int | None = 0,
            image_fmt: str = "path",

    ) -> Generator[dict, None, None]:
        tmpdir = self.mk_tmpdir()
        if max_workers == 0:
            with fitz.open(filepath) as doc:
                page_total = len(doc)
                for page_idx in range(page_total):
                    for item in self._process_page(
                            doc=doc,
                            page_idx=page_idx,
                            page_total=page_total,
                            tmpdir=tmpdir,
                            keep_page_image=keep_page_image,
                            keep_emdb_image=keep_emdb_image,
                            dpi=dpi,
                            image_fmt=image_fmt,
                    ):
                        yield item
                return
        kv = KValue()
        max_workers = max_workers or cpu_count()
        with fitz.open(filepath) as doc:
            page_total = len(doc)
        results, next_page_idx = {}, 0
        with ProcessPoolExecutor(max_workers=max_workers) as executor:
            futures = [
                executor.submit(self._process_and_save_page, **{
                    "filepath": filepath,
                    "page_idx": page_idx,
                    "page_total": page_total,
                    "tmpdir": tmpdir,
                    "keep_page_image": keep_page_image,
                    "keep_emdb_image": keep_emdb_image,
                    "dpi": dpi,
                    "image_fmt": image_fmt,
                    "kvfile": kv.file,
                })
                for page_idx in range(page_total)
            ]
            for future in as_completed(futures):
                page_idx, data = future.result()
                results[page_idx] = data
                while next_page_idx in results:
                    for key in results.pop(next_page_idx):
                        yield kv.get(key)
                    next_page_idx += 1
        kv.remove()
        self.rm_empty_dir(tmpdir)

    def _process_and_save_page(
            self,
            filepath: str,
            page_idx: int,
            page_total: int,
            tmpdir: str,
            keep_page_image: bool,
            keep_emdb_image: bool,
            dpi: int,
            image_fmt: str,
            kvfile: str,
    ) -> tuple[int, list]:
        kv = KValue(file=kvfile)
        with fitz.open(filepath) as doc:
            data, idx = [], 0
            for item in self._process_page(
                    doc=doc,
                    page_idx=page_idx,
                    page_total=page_total,
                    tmpdir=tmpdir,
                    keep_page_image=keep_page_image,
                    keep_emdb_image=keep_emdb_image,
                    dpi=dpi,
                    image_fmt=image_fmt,
            ):
                key = f"{page_idx}.{idx}"
                kv.set(key, item)
                data.append(key)
                idx += 1
            return page_idx, data

    def _process_page(
            self,
            doc,
            page_idx: int,
            page_total: int,
            tmpdir: str,
            keep_page_image: bool,
            keep_emdb_image: bool,
            dpi: int,
            image_fmt: str,
    ) -> Generator[dict, None, None]:
        page = doc.load_page(page_idx)
        page_num = page_idx + 1
        page_path = None
        if keep_page_image:
            page_pix = page.get_pixmap(dpi=dpi, alpha=False)
            ext = "png" if page_pix.alpha else "jpg"
            page_path = os.path.join(tmpdir, f"image_{page_idx}.{ext}")
            try:
                page_pix.save(page_path)
            except Exception as e:
                self.rm_file(page_path)
                page_path = None
                logger.error(f"Failed to save image: {e}")
            finally:
                if 'page_pix' in locals():
                    del page_pix
        if self._is_two_column(page):
            page_text = self._extract_adaptive_columns(page)
        else:
            page_text = page.get_text("text")
        yield {
            "type": "text",
            "text": page_text,
            "page": page_num,
            "page_total": page_total,
            "page_path": page_path,
        }
        if keep_emdb_image:
            for img_idx, img in enumerate(page.get_images(full=True)):
                xref = img[0]
                pix = fitz.Pixmap(doc, xref)
                if pix.colorspace not in (fitz.csGRAY, fitz.csRGB, fitz.csCMYK):
                    pix = fitz.Pixmap(fitz.csRGB, pix)
                    ext = "png"
                else:
                    ext = "png" if pix.alpha else "jpg"
                image_path = os.path.join(tmpdir, f"image_{page_idx}-{img_idx}.{ext}")
                try:
                    pix.save(image_path)
                    yield {
                        "type": "image",
                        "text": format_image(image_path, fmt=image_fmt),  # noqa
                        "data": image_path,
                        "page": page_num,
                        "page_total": page_total,
                        "page_path": page_path,
                    }
                except Exception as e:
                    self.rm_file(image_path)
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

    @staticmethod
    def extract_by_pdfplumber(
            filepath: str,
    ) -> Generator[dict, None, None]:
        with pdfplumber.open(filepath) as pdf:
            page_total = len(pdf.pages)
            for page in pdf.pages:
                page_num = page.page_number
                page_path = None
                text = page.extract_text()
                yield {
                    "type": "text",
                    "text": text,
                    "page": page_num,
                    "page_total": page_total,
                    "page_path": page_path,
                }
                for table_data in page.extract_tables():
                    yield {
                        "type": "table",
                        "text": format_table(table_data),
                        "data": table_data,
                        "page": page_num,
                        "page_total": page_total,
                        "page_path": page_path,
                    }
