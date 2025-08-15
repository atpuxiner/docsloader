import logging
from typing import AsyncGenerator

import aiofiles

from docsloader._base import BaseLoader

logger = logging.getLogger(__name__)


class TxtLoader(BaseLoader):

    async def load(self) -> AsyncGenerator[dict, None]:
        async with aiofiles.open(await self.tmp_file, mode='r', encoding=self.encoding) as f:
            idx = 0
            async for line in f:
                idx += 1
                yield {
                    "line": idx,
                    "text": line,
                }
        await self.remove_tmp_file()
