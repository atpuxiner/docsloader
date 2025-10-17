import logging
import sys
from typing import Generator

from docsloader import utils

try:
    import pdfplumber
except ImportError:
    sys.stderr.write(
        "Run installing with: "
        "pip install pdfplumber"
        "\n<<<\n"
    )
    raise

logger = logging.getLogger(__name__)


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
                    "text": utils.format_table(table_data),
                    "data": table_data,
                    "page": page_num,
                    "page_total": page_total,
                    "page_path": page_path,
                }
