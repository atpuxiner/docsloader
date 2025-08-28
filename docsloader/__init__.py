"""
@author axiner
@version v0.0.1
@created 2025/08/15 09:06
@abstract
@description
@history
"""
import os

from ._txt import TxtLoader
from ._csv import CsvLoader
from ._md import MdLoader
from ._html import HtmlLoader
from ._xlsx import XlsxLoader
from ._pptx import PptxLoader
from ._docx import DocxLoader
from ._pdf import PdfLoader
from ._img import ImgLoader

__version__ = "0.0.3"

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

_SUFFIX_TO_MODEL = {
    ".txt": "_txt",
    ".csv": "_csv",
    ".md": "_md",
    ".html": "_html",
    ".htm": "_html",
    ".xlsx": "_xlsx",
    ".xls": "_xlsx",
    ".pptx": "_pptx",
    ".ppt": "_pptx",
    ".docx": "_docx",
    ".doc": "_docx",
    ".pdf": "_pdf",
    ".jpg": "_img",
    ".jpeg": "_img",
    ".png": "_img",
}


class AutoLoader:
    """
    auto loader
    """

    def __new__(
            cls,
            path_or_url: str,
            suffix: str = None,
            encoding: str = None,
            load_type: str = "basic",
            load_options: dict = None,
            metadata: dict = None,
            rm_tmpfile: bool = False,
    ):
        """自动根据 suffix 返回对应的 Loader 实例"""
        if suffix is None:
            _, suffix = os.path.splitext(path_or_url)
            if not suffix:
                raise ValueError("无法从`path_or_url`推断文件后缀，请显式指定`suffix`")
        else:
            if not suffix.startswith('.'):
                suffix = f".{suffix}"
        suffix = suffix.lower()
        loader_class = cls._get_loader_class(suffix)
        return loader_class(
            path_or_url=path_or_url,
            suffix=suffix,
            encoding=encoding,
            load_type=load_type,
            load_options=load_options,
            metadata=metadata,
            rm_tmpfile=rm_tmpfile,
        )

    @staticmethod
    def _get_loader_class(suffix: str):
        """get loader class"""
        if suffix not in _SUFFIX_TO_MODEL:
            raise ValueError(f"不支持的文件后缀: {suffix}")
        model_name = _SUFFIX_TO_MODEL[suffix]
        class_name = f"{model_name.replace('_', '').title()}Loader"
        loader_class = getattr(
            __import__(f"docsloader.{model_name}", fromlist=[model_name]),
            class_name
        )
        return loader_class
