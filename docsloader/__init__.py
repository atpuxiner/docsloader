"""
@author axiner
@version v0.0.1
@created 2025/08/15 09:06
@abstract
@description
@history
"""
import importlib
from typing import TYPE_CHECKING

__version__ = "0.0.8"

__all__ = [
    "TxtLoader",
    "CsvLoader",
    "MdLoader",
    "HtmlLoader",
    "XlsxLoader",
    "PptxLoader",
    "DocxLoader",
    "PdfLoader",
    "ImgLoader",
    "AutoLoader",
]

if TYPE_CHECKING:
    from docsloader._txt import TxtLoader
    from docsloader._csv import CsvLoader
    from docsloader._md import MdLoader
    from docsloader._html import HtmlLoader
    from docsloader._xlsx import XlsxLoader
    from docsloader._pptx import PptxLoader
    from docsloader._docx import DocxLoader
    from docsloader._pdf import PdfLoader
    from docsloader._img import ImgLoader
    from docsloader._auto import AutoLoader


def __getattr__(name):
    if name in __all__:
        module = importlib.import_module(f"docsloader._{name.replace('Loader', '').lower()}")
        return getattr(module, name)
    raise AttributeError(f"ImportError: cannot import name '{name}' from 'docsloader'")
