import logging
from typing import AsyncGenerator

from rapidocr_onnxruntime import RapidOCR

from docsloader.base import BaseLoader, DocsData

logger = logging.getLogger(__name__)


class ImgLoader(BaseLoader):
    """
    img loader

    params:
        - path_or_url: str
        - suffix: str = None
        - encoding: str = None
        - load_type: str = "basic"
        - load_options: dict = None
        - metadata: dict = None
        - rm_tmpfile: bool = False
    """

    async def load_by_basic(self) -> AsyncGenerator[DocsData, None]:
        ocr = RapidOCR()
        res, _ = ocr(self.tmpfile)
        if res:
            for item in res:
                yield DocsData(
                    type="text",
                    text=item[1],
                    metadata=self.metadata,
                )
        else:
            yield DocsData(
                type="text",
                text="",
                metadata=self.metadata,
            )
