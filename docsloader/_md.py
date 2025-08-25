import logging
from typing import AsyncGenerator

import aiofiles

from docsloader.base import BaseLoader, DocsData

logger = logging.getLogger(__name__)


class MdLoader(BaseLoader):

    async def load_by_basic(self) -> AsyncGenerator[DocsData, None]:
        async with aiofiles.open(self.tmpfile, mode="r", encoding=self.encoding) as f:
            async for line in f:
                yield DocsData(
                    type="text",
                    text=line,
                    metadata=self.metadata,
                )
