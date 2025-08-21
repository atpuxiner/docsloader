import logging
from pathlib import Path
from typing import AsyncGenerator

import fitz
import numpy as np

from docsloader.base import BaseLoader, DocsData

logger = logging.getLogger(__name__)


class PdfLoader(BaseLoader):

    async def load_by_basic(self) -> AsyncGenerator[DocsData, None]:
        try:
            idx = 0
            for item in self.extract_by_pymupdf(tmpfile=await self.tmpfile):
                metadata = self.metadata.copy()
                metadata.update(
                    idx=idx,
                    page=item.get("page"),
                    page_total=item.get("page_total"),
                    page_path=item.get("page_path"),
                    image_path=item.get("image_path"),
                )
                yield DocsData(
                    type=item.get("type"),
                    text=item.get("text"),
                    data=item.get("data"),
                    metadata=metadata,
                )
                idx += 1
        finally:
            await self.rm_tmpfile()

    def extract_by_pymupdf(self, tmpfile: str, dpi: int = 300):
        doc = fitz.open(tmpfile)
        image_dir = Path(tmpfile + ".images")
        image_dir.mkdir(parents=True, exist_ok=True)
        page_total = len(doc)
        for page_idx in range(page_total):
            page = doc.load_page(page_idx)
            page_pix = page.get_pixmap(dpi=dpi, alpha=False)
            ext = "png" if page_pix.alpha else "jpg"
            page_path = image_dir / f"image_{page_idx}.{ext}"
            try:
                page_pix.save(str(page_path))
            except Exception as e:
                page_path = None
                logger.error(f"Failed to save image: {e}")
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
                    "page_path": str(page_path),
                }
            # image
            for img_idx, img in enumerate(page.get_images(full=True)):
                xref = img[0]
                pix = fitz.Pixmap(doc, xref)
                ext = "png" if pix.alpha else "jpg"
                image_path = image_dir / f"image_{page_idx}-{img_idx}.{ext}"
                try:
                    pix.save(str(image_path))
                    yield {
                        "type": "image",
                        "page": page_idx + 1,
                        "page_total": page_total,
                        "page_path": str(page_path),
                        "image_path": str(image_path),
                    }
                except Exception as e:
                    logger.error(f"Failed to save image: {e}")

    @staticmethod
    def _is_two_column(page, margin_threshold=0.1):
        """检测页面是否为双列布局"""
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
    def _extract_adaptive_columns(page):
        """自适应分列提取文本"""
        text_blocks = [b for b in page.get_text("blocks") if b[4].strip()]
        if not text_blocks:
            return ""

        x_coords = sorted([(b[0] + b[2]) / 2 for b in text_blocks])
        gaps = [x_coords[i + 1] - x_coords[i] for i in range(len(x_coords) - 1)]
        max_gap_index = np.argmax(gaps)
        split_x = (x_coords[max_gap_index] + x_coords[max_gap_index + 1]) / 2

        left_col = []
        right_col = []
        for b in sorted(text_blocks, key=lambda x: (-x[1], x[0])):
            block_center = (b[0] + b[2]) / 2
            if block_center < split_x:
                left_col.append(b[4])
            else:
                right_col.append(b[4])

        return "\n".join(left_col + right_col)
