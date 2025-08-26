"""
@author axiner
@version v0.0.1
@created 2025/08/15 09:06
@abstract
@description
@history
"""
from ._txt import TxtLoader
from ._csv import CvsLoader
from ._md import MdLoader
from ._html import HtmlLoader
from ._xlsx import XlsxLoader
from ._pptx import PptxLoader
from ._docx import DocxLoader
from ._pdf import PdfLoader
from ._img import ImgLoader

__version__ = "0.0.2"

__all__ = [
    "TxtLoader",
    "CvsLoader",
    "MdLoader",
    "HtmlLoader",
    "XlsxLoader",
    "PptxLoader",
    "DocxLoader",
    "PdfLoader",
    "ImgLoader",
]
