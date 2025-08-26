import logging
from typing import AsyncGenerator

from rapidocr_onnxruntime import RapidOCR

from docsloader.base import BaseLoader, DocsData

logger = logging.getLogger(__name__)


class ImgLoader(BaseLoader):

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
