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
            encoding: str = "utf-8",
            is_detect_encoding: bool = False
    ):
        self.path_or_url = path_or_url
        self.encoding = encoding
        self.is_detect_encoding = is_detect_encoding
        self._tmp_file = None

    async def load(self) -> AsyncGenerator[dict, None]:
        """加载"""
        raise NotImplementedError("subclasses of BaseLoader must provide a load() method")

    @property
    async def tmp_file(self):
        """临时文件"""
        if self._tmp_file:
            return self._tmp_file
        self._tmp_file = self.path_or_url
        result = urlparse(self.path_or_url)
        if all([result.scheme, result.netloc]):  # url
            logger.info(f"downloading {self.path_or_url} to temp file")
            self._tmp_file = await download_to_tmpfile(self.path_or_url)
        if self.is_detect_encoding:
            self.encoding = detect_encoding(data_or_path=self._tmp_file)
        return self._tmp_file

    async def remove_tmp_file(self):
        """删除临时文件"""
        if self._tmp_file and os.path.exists(self._tmp_file):
            os.remove(self._tmp_file)
