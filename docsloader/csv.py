import json
import logging
from typing import AsyncGenerator

import pandas as pd

from docsloader.base import BaseLoader

logger = logging.getLogger(__name__)


class CvsLoader(BaseLoader):

    async def load_by_basic(self, sep: str = ",") -> AsyncGenerator[dict, None]:
        df = pd.read_csv(await self.tmpfile, sep=sep, encoding=self.encoding)
        columns = df.columns.tolist()
        try:
            for idx, row in df.iterrows():
                yield {
                    "idx": idx,
                    "text": json.dumps(row.to_dict(), ensure_ascii=False),
                    "columns": columns,
                }
        finally:
            await self.rm_tmpfile()
