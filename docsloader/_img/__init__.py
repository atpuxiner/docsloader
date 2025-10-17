from typing import AsyncGenerator

from docsloader.base import BaseLoader, DocsData


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
        from ._rapidocr import extract_by_rapidocr
        result = extract_by_rapidocr(
            filepath=self.tmpfile,
            load_options=self.load_options,
        )
        if result:
            for item in result:
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

    async def load_by_tesseract(self) -> AsyncGenerator[DocsData, None]:
        from ._tesseract import extract_by_tesseract
        result = extract_by_tesseract(
            filepath=self.tmpfile,
            load_options=self.load_options,
        )
        if result:
            for text in result:
                yield DocsData(
                    type="text",
                    text=text,
                    metadata=self.metadata,
                )
        else:
            yield DocsData(
                type="text",
                text="",
                metadata=self.metadata,
            )
