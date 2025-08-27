import logging
import os
import shutil
from typing import AsyncGenerator, Any
from urllib.parse import urlparse

from pydantic import BaseModel
from toollib.codec import detect_encoding

from docsloader.utils import download_to_tmpfile

logger = logging.getLogger(__name__)


class DocsData(BaseModel):
    """文档数据"""
    idx: int | None = None
    type: str | None = None
    text: str | None = None
    data: Any = None
    metadata: dict | None = None


class BaseLoader:
    """
    base loader
    """

    def __init__(
            self,
            path_or_url: str,
            suffix: str = None,
            encoding: str = None,
            load_type: str = "basic",
            load_options: dict = None,
            metadata: dict = None,
            is_rm_tmpfile: bool = True
    ):
        self.path_or_url = path_or_url
        self.suffix = suffix
        self.encoding = encoding
        self.load_type = load_type
        self.load_options = load_options or {}
        self.metadata = metadata or {}
        self.is_rm_tmpfile = is_rm_tmpfile
        self.tmpfile = None

    async def load(self, **kwargs) -> AsyncGenerator[DocsData, None]:
        """加载"""
        load_type = kwargs.pop("load_type", self.load_type)
        logger.info(f"load type: {load_type}")
        if method := getattr(self, f"load_by_{load_type}", None):
            try:
                await self.setup()
                if await self.is_file_empty(self.tmpfile):
                    logger.warning(f"File is empty({self.path_or_url}): {self.tmpfile}")
                    yield DocsData(type="empty")
                    return
                self.load_options.update(kwargs)
                idx = 0
                async for item in method():
                    item.idx = idx
                    yield item
                    idx += 1
            finally:
                await self.rm_tmpfile()
        else:
            raise ValueError(f"Unsupported load type: {load_type}")

    async def setup(self):
        """初始化"""
        if self.tmpfile is not None:
            return
        self.tmpfile = self.path_or_url
        if self.suffix is None:
            _, self.suffix = os.path.splitext(self.tmpfile)
        res = urlparse(self.path_or_url)
        if all([res.scheme, res.netloc]):  # url
            logger.info(f"downloading {self.path_or_url} to tmpfile")
            self.tmpfile = await download_to_tmpfile(url=self.path_or_url, suffix=self.suffix)
        if not self.encoding:
            self.encoding = detect_encoding(data_or_path=self.tmpfile)
        # load options
        self.load_options.setdefault("csv_sep", ",")
        self.load_options.setdefault("html_exclude_tags", ("script", "style"))
        self.load_options.setdefault("html_remove_blank_text", True)
        self.load_options.setdefault("pdf_dpi", 300)
        self.load_options.setdefault("image_fmt", "path")
        self.load_options.setdefault("table_fmt", "html")

    @staticmethod
    async def is_file_empty(file_path) -> bool:
        return os.path.getsize(file_path) == 0

    async def rm_tmpfile(self):
        """删除临时文件"""
        if self.is_rm_tmpfile:
            if self.tmpfile and os.path.exists(self.tmpfile):
                os.remove(self.tmpfile)

    @staticmethod
    async def rm_tmpdir(tmpdir: str):
        """删除临时目录"""
        if os.path.isdir(tmpdir):
            shutil.rmtree(tmpdir)
