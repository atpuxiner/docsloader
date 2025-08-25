import base64
import logging
from pathlib import Path
from typing import Literal

import aiohttp
import aiofiles
import tempfile

logger = logging.getLogger(__name__)


async def download_to_tmpfile(
        url: str,
        suffix: str = None,
        timeout: int = 120,
) -> str:
    """download to tmpfile"""
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as f:
        tmp_file = Path(f.name)
    try:
        timeout = aiohttp.ClientTimeout(total=timeout)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(url) as response:
                if response.status != 200:
                    text = await response.text()
                    msg = f"{response.status} {text}"
                    raise ValueError(msg)
                async with aiofiles.open(tmp_file, 'wb') as f:
                    async for chunk in response.content.iter_any():
                        await f.write(chunk)  # noqa
                return str(tmp_file)
    except Exception as e:
        logger.error(e)
        if tmp_file.exists():
            tmp_file.unlink(missing_ok=True)
        raise


def format_table(
        table: list,
        fmt: Literal["html", "md"] = 'html',
) -> str:
    """format table"""
    if not table:
        return ""
    if isinstance(table[0], str):
        if fmt == 'md':
            return "| " + " | ".join(map(str, table)) + " |"
        else:
            return "<tr>" + "".join(f"<td>{r}</td>" for r in map(str, table)) + "</tr>"
    headers = table[0] if not isinstance(table[0], str) else table
    if fmt == 'md':
        md = "| " + " | ".join(map(str, headers)) + " |\n"
        md += "| " + " | ".join(["---"] * len(headers)) + " |\n"
        for row in table[1:]:
            md += "| " + " | ".join(map(str, row)) + " |\n"
        return md
    else:
        html = "<table>"
        html += "".join(f"<th>{h}</th>" for h in map(str, headers))
        for row in table[1:]:
            html += "<tr>" + "".join(f"<td>{d}</td>" for d in map(str, row)) + "</tr>"
        html += "</table>"
        return html


def format_image(
        image_path: str,
        alt_text: str = "Image",
        fmt: Literal["path", "base64"] = "path",
) -> str:
    """format image"""
    image_path = Path(image_path)
    if fmt == "base64":
        with open(image_path, "rb") as f:
            encoded_img = base64.b64encode(f.read()).decode()
        mime_type = {
            'jpg': 'jpeg',
            'jpeg': 'jpeg',
            'png': 'png',
            'gif': 'gif',
            'svg': 'svg+xml',
        }.get(image_path.suffix.lower()[1:], 'png')
        return f"![{alt_text}](data:image/{mime_type};base64,{encoded_img})"
    abs_path = str(image_path.absolute()).replace('\\', '/')
    return f"![{alt_text}](file:///{abs_path})"
