import logging
from typing import AsyncGenerator

import aiofiles

from docsloader.base import BaseLoader

logger = logging.getLogger(__name__)


class TxtLoader(BaseLoader):

    async def load_by_basic(self) -> AsyncGenerator[dict, None]:
        async with aiofiles.open(await self.tmpfile, mode='r', encoding=self.encoding) as f:
            idx = 0
            async for line in f:
                self.metadata.update(
                    idx=idx,
                )
                yield {
                    "type": "text",
                    "text": line,
                    "metadata": self.metadata,
                }
                idx += 1
        await self.rm_tmpfile()
