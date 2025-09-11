import logging
from typing import AsyncGenerator, Generator

from lxml import etree

from docsloader.base import BaseLoader, DocsData

logger = logging.getLogger(__name__)


class HtmlLoader(BaseLoader):
    """
    html loader

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
        html_exclude_tags = self.load_options.get("html_exclude_tags")
        html_remove_blank_text = self.load_options.get("html_remove_blank_text")
        for item in self.extract_by_lxml(
                filepath=self.tmpfile,
                exclude_tags=html_exclude_tags,
                remove_blank_text=html_remove_blank_text,
        ):
            yield DocsData(
                type=item.get("type"),
                text=item.get("text"),
                data=item.get("data"),
                metadata=self.metadata,
            )

    @staticmethod
    def extract_by_lxml(
            filepath: str,
            exclude_tags: tuple | None = ("script", "style"),
            remove_blank_text: bool = True,
    ) -> Generator[dict, None, None]:
        context = etree.iterparse(filepath, events=('end',), html=True, remove_blank_text=remove_blank_text)
        for event, element in context:
            if exclude_tags and element.tag in exclude_tags:
                continue
            if element.text and element.text.strip():
                yield {
                    "type": element.tag,
                    "text": element.text.strip(),
                }
            element.clear()
            parent = element.getparent()
            if parent is not None:
                while len(parent) > 0:
                    del parent[0]
