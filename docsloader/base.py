import logging
import os
from typing import AsyncGenerator
from urllib.parse import urlparse

from toollib.codec import detect_encoding

from docsloader.utils import download_to_tmpfile

logger = logging.getLogger(__name__)


class BaseLoader:

    def __init__(
            self,
            path_or_url: str,
            encoding: str = None,
            load_type: str = "basic",
            is_rm_tmpfile: bool = True
    ):
        self.path_or_url = path_or_url
        self.encoding = encoding
        self.load_type = load_type
        self.is_rm_tmpfile = is_rm_tmpfile
        self._tmpfile = None

    async def load(self, *args, **kwargs) -> AsyncGenerator[dict, None]:
        """加载"""
        load_type = kwargs.get("load_type") or self.load_type
        logger.info(f"load type: {load_type}")
        if method := getattr(self, f"load_by_{load_type}", None):
            async for item in method(*args, **kwargs):
                yield item
        else:
            raise ValueError(f"Unsupported load type: {load_type}")

    @property
    async def tmpfile(self):
        """临时文件"""
        if self._tmpfile:
            return self._tmpfile
        self._tmpfile = self.path_or_url
        result = urlparse(self.path_or_url)
        if all([result.scheme, result.netloc]):  # url
            logger.info(f"downloading {self.path_or_url} to tmpfile")
            self._tmpfile = await download_to_tmpfile(self.path_or_url)
        if not self.encoding:
            self.encoding = detect_encoding(data_or_path=self._tmpfile)
        return self._tmpfile

    async def rm_tmpfile(self):
        """删除临时文件"""
        if self.is_rm_tmpfile:
            if self._tmpfile and os.path.exists(self._tmpfile):
                os.remove(self._tmpfile)
