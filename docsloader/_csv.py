import csv
import logging
from typing import AsyncGenerator

from docsloader.base import BaseLoader, DocsData

logger = logging.getLogger(__name__)


class CvsLoader(BaseLoader):

    async def load_by_basic(self, sep: str = ",") -> AsyncGenerator[DocsData, None]:
        with open(self.tmpfile, "r", encoding=self.encoding, newline="") as f:  # header
            reader = csv.reader(f, delimiter=sep)
            try:
                header = [col.strip() or None for col in next(reader)]
            except StopIteration:
                return
            header_len = len(header)
            for row in reader:
                if len(row) > header_len:
                    header_len = len(row)
            if len(header) < header_len:
                header.extend([None] * (header_len - len(header)))  # noqa
        with open(self.tmpfile, "r", encoding=self.encoding, newline="") as f:  # body
            reader = csv.reader(f, delimiter=sep)
            try:
                next(reader)
            except StopIteration:
                return
            idx = 0
            self.metadata.update(
                header=header,
            )
            for idx, row in enumerate(reader):
                row = [r if r else None for r in row]
                padded_row = (row + [None] * (header_len - len(row)))[:header_len]
                yield DocsData(
                    idx=idx,
                    type="text",
                    data=padded_row,
                    metadata=self.metadata,
                )
            if not idx:
                yield DocsData(
                    idx=0,
                    type="text",
                    data=[],
                    metadata=self.metadata,
                )
