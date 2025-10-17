import logging
import sys

try:
    from rapidocr_onnxruntime import RapidOCR
except ImportError:
    sys.stderr.write(
        "Run installing with: "
        "pip install docsloader[img]"
        "\n<<<\n"
    )
    raise

logger = logging.getLogger(__name__)


def extract_by_rapidocr(
        filepath: str,
        load_options: dict,
) -> list:
    img_preprocess = load_options.get("img_preprocess")
    ocr = RapidOCR()
    if callable(img_preprocess):
        img = img_preprocess(filepath)
    else:
        img = filepath
    result, _ = ocr(img)
    if not result:
        return []
    return result
