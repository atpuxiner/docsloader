import logging
import sys

try:
    import pytesseract
except ImportError:
    sys.stderr.write(
        "Run installing with: "
        "pip install pytesseract"
        "\n<<<\n"
    )
    raise

logger = logging.getLogger(__name__)


def extract_by_tesseract(
        filepath: str,
        load_options: dict,
) -> list:
    img_preprocess = load_options.get("img_preprocess")
    img_tesseract_cmd = load_options.get("img_tesseract_cmd")
    img_lang = load_options.get("img_lang")
    img_config = load_options.get("img_config")
    img_nice = load_options.get("img_nice")
    if img_tesseract_cmd:
        pytesseract.pytesseract.tesseract_cmd = img_tesseract_cmd
    if callable(img_preprocess):
        img = img_preprocess(filepath)
    else:
        img = filepath
    result = pytesseract.image_to_string(
        image=img,
        lang=img_lang,
        config=img_config,
        nice=img_nice,
    )
    if not result:
        return []
    return result.strip().split('\n')
