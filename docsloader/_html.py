import logging
from typing import AsyncGenerator, Generator

from lxml import etree

from docsloader.base import BaseLoader, DocsData

logger = logging.getLogger(__name__)


class HtmlLoader(BaseLoader):

    async def load_by_basic(self) -> AsyncGenerator[DocsData, None]:
        for item in self.extract_by_lxml(tmpfile=self.tmpfile):
            yield DocsData(
                type=item.get("type"),
                text=item.get("text"),
                data=item.get("data"),
                metadata=self.metadata,
            )

    @staticmethod
    def extract_by_lxml(
            tmpfile: str,
            exclude_tags: tuple | None = ("script", "style"),
            is_remove_blank_text: bool = True,
    ) -> Generator[dict, None, None]:
        context = etree.iterparse(tmpfile, events=('end',), html=True, remove_blank_text=is_remove_blank_text)
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
