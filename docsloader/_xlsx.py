import logging
from typing import AsyncGenerator

import pandas as pd

from docsloader.base import BaseLoader, DocsData

logger = logging.getLogger(__name__)


class XlsxLoader(BaseLoader):

    async def load_by_basic(self) -> AsyncGenerator[DocsData, None]:
        df = pd.read_excel(await self.tmpfile, engine="openpyxl")
        columns = df.columns.tolist()
        try:
            for idx, row in df.iterrows():
                self.metadata.update(
                    idx=idx,
                    columns=columns,
                )
                yield DocsData(
                    type="text",
                    data=row.to_dict(),
                    metadata=self.metadata,
                )
        finally:
            await self.rm_tmpfile()
