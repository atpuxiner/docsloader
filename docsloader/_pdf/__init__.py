from typing import AsyncGenerator

from docsloader.base import BaseLoader, DocsData


class PdfLoader(BaseLoader):
    """
    pdf loader

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
        from ._pymupdf import extract_by_pymupdf
        for item in extract_by_pymupdf(
                filepath=self.tmpfile,
                load_options=self.load_options,
        ):
            self.metadata.update({
                "page": item.get("page"),
                "page_total": item.get("page_total"),
                "page_path": item.get("page_path"),
            })
            yield DocsData(
                type=item.get("type"),
                text=item.get("text"),
                data=item.get("data"),
                metadata=self.metadata,
            )

    async def load_by_pdfplumber(self) -> AsyncGenerator[DocsData, None]:
        from ._pdfplumber import extract_by_pdfplumber
        for item in extract_by_pdfplumber(filepath=self.tmpfile):
            self.metadata.update({
                "page": item.get("page"),
                "page_total": item.get("page_total"),
                "page_path": item.get("page_path"),
            })
            yield DocsData(
                type=item.get("type"),
                text=item.get("text"),
                data=item.get("data"),
                metadata=self.metadata,
            )
