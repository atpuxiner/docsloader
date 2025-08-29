# docsloader

## What is this?

- by: axiner
- docsloader
- This is a documents loader.

## Installation

This package can be installed using pip (Python>=3.11):
> pip install docsloader

## Usage

The `docsloader` package provides asynchronous document loaders for various file suffixes. It includes dedicated loaders
for specific file types and an `AutoLoader` that automatically selects the appropriate loader based on file suffix.

### Supported File Suffixes

The package supports loading documents from the following file suffixes:

- **Text Files**: `.txt`
- **CSV Files**: `.csv`
- **Markdown Files**: `.md`
- **HTML Files**: `.html`, `.htm`
- **Excel Files**: `.xlsx`, `.xls`
- **PowerPoint Files**: `.pptx`, `.ppt`
- **Word Files**: `.docx`, `.doc`
- **PDF Files**: `.pdf`
- **Image Files**: `.jpg`, `.jpeg`, `.png`

### Available Loaders

The package provides the following loader classes:

- `TxtLoader`: For Text files
- `CsvLoader`: For CSV files
- `MdLoader`: For Markdown files
- `HtmlLoader`: For HTML files
- `XlsxLoader`: For Excel files
- `PptxLoader`: For PowerPoint files
- `DocxLoader`: For Word files
- `PdfLoader`: For PDF files
- `ImgLoader`: For image files
- `AutoLoader`: Automatically selects the appropriate loader based on file suffix

All loader classes implement asynchronous `load` methods for efficient document processing.

### Example

```python
import asyncio

from docsloader import AutoLoader
from toollib.log import init_logger

logger = init_logger(__name__)


async def main(path_or_url: str):
    loader = AutoLoader(
        path_or_url=path_or_url,
        rm_tmpfile=False,
    )
    async for doc in loader.load():
        logger.info(doc)


if __name__ == "__main__":
    asyncio.run(main(path_or_url=r"E:/NewFolder/测试.docx"))
```

## License

This project is released under the MIT License (MIT). See [LICENSE](LICENSE)
