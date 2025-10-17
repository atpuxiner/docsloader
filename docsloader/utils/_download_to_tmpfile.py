import logging
import tempfile
from pathlib import Path

import aiofiles
import aiohttp

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
