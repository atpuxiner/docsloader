import logging
import os
import sys
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Generator

from toollib.kvalue import KValue

from docsloader import utils

try:
    import fitz
    import numpy as np
except ImportError:
    sys.stderr.write(
        "Run installing with: "
        "pip install docsloader[pdf]"
        "\n<<<\n"
    )
    raise

logger = logging.getLogger(__name__)


def extract_by_pymupdf(
        filepath: str,
        load_options: dict,
) -> Generator[dict, None, None]:
    pdf_keep_page_image = load_options.get("pdf_keep_page_image")
    pdf_keep_emdb_image = load_options.get("pdf_keep_emdb_image")
    pdf_dpi = load_options.get("pdf_dpi")
    max_workers = load_options.get("max_workers")
    image_fmt = load_options.get("image_fmt")
    tmpdir = utils.mk_tmpdir()
    if max_workers == 0:
        with fitz.open(filepath) as doc:
            page_total = len(doc)
            for page_idx in range(page_total):
                for item in _process_page(
                        doc=doc,
                        page_idx=page_idx,
                        page_total=page_total,
                        tmpdir=tmpdir,
                        keep_page_image=pdf_keep_page_image,
                        keep_emdb_image=pdf_keep_emdb_image,
                        dpi=pdf_dpi,
                        image_fmt=image_fmt,
                ):
                    yield item
            return
    kv = KValue()
    max_workers = max_workers or os.cpu_count()
    with fitz.open(filepath) as doc:
        page_total = len(doc)
    results, next_page_idx = {}, 0
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        futures = [
            executor.submit(_process_and_save_page, **{
                "filepath": filepath,
                "page_idx": page_idx,
                "page_total": page_total,
                "tmpdir": tmpdir,
                "keep_page_image": pdf_keep_page_image,
                "keep_emdb_image": pdf_keep_emdb_image,
                "dpi": pdf_dpi,
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
    utils.rm_empty_dir(tmpdir)


def _process_and_save_page(
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
        for item in _process_page(
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
            utils.rm_file(page_path)
            page_path = None
            logger.error(f"Failed to save image: {e}")
        finally:
            if 'page_pix' in locals():
                del page_pix
    if _is_two_column(page):
        page_text = _extract_adaptive_columns(page)
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
                    "text": utils.format_image(image_path, fmt=image_fmt),  # noqa
                    "data": image_path,
                    "page": page_num,
                    "page_total": page_total,
                    "page_path": page_path,
                }
            except Exception as e:
                utils.rm_file(image_path)
                logger.error(f"Failed to save image: {e}")
            finally:
                if 'pix' in locals():
                    del pix


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
