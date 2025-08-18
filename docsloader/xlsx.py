import json
import logging
from typing import AsyncGenerator

import pandas as pd

from docsloader.base import BaseLoader

logger = logging.getLogger(__name__)


class XlsxLoader(BaseLoader):

    async def load_by_basic(self) -> AsyncGenerator[dict, None]:
        df = pd.read_excel(await self.tmpfile)
        columns = df.columns.tolist()
        try:
            for idx, row in df.iterrows():
                self.metadata.update(
                    idx=idx,
                    columns=columns,
                )
                yield {
                    "text": json.dumps(row.to_dict(), ensure_ascii=False),
                    "metadata": self.metadata,
                }
        finally:
            await self.rm_tmpfile()
