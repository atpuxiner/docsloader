import csv
import logging
from typing import AsyncGenerator

from docsloader.base import BaseLoader, DocsData
from docsloader.utils import format_table

logger = logging.getLogger(__name__)


class CvsLoader(BaseLoader):

    async def load_by_basic(self) -> AsyncGenerator[DocsData, None]:
        csv_sep = self.load_options.get("csv_sep")
        table_fmt = self.load_options.get("table_fmt")
        with open(self.tmpfile, "r", encoding=self.encoding, newline="") as f:  # header
            reader = csv.reader(f, delimiter=csv_sep)
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
            reader = csv.reader(f, delimiter=csv_sep)
            try:
                next(reader)
            except StopIteration:
                return
            self.metadata.update({
                "header": header,
            })
            has_value = False
            for row in reader:
                has_value = True
                row = [r if r else None for r in row]
                row = (row + [None] * (header_len - len(row)))[:header_len]
                yield DocsData(
                    type="text",
                    text=format_table(row, fmt=table_fmt),
                    data=row,
                    metadata=self.metadata,
                )
            if not has_value:
                yield DocsData(
                    type="text",
                    text="",
                    data=[],
                    metadata=self.metadata,
                )
