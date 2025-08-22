"""
@author axiner
@version v0.0.1
@created 2025/08/15 09:06
@abstract
@description
@history
"""
from ._txt import TxtLoader
from ._md import MdLoader
from ._csv import CvsLoader
from ._xlsx import XlsxLoader
from ._docx import DocxLoader
from ._pdf import PdfLoader

__version__ = "0.0.2"
__all__ = [
    "TxtLoader",
    "MdLoader",
    "CvsLoader",
    "XlsxLoader",
    "DocxLoader",
    "PdfLoader",
]
