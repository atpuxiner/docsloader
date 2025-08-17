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
                idx += 1
                yield {
                    "idx": idx,
                    "text": line,
                }
        await self.rm_tmpfile()
