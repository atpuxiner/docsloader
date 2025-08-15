import logging

import aiohttp
import aiofiles
import tempfile
from pathlib import Path

logger = logging.getLogger(__name__)


async def download_to_tmpfile(
        url: str,
        suffix: str = None,
        timeout=60,
) -> str:
    """
    下载URL内容到临时文件
    :param url: url
    :param suffix: 文件后缀
    :param timeout: 超时时间
    :return: 临时文件
    """
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
                        await f.write(chunk)
                return str(tmp_file)
    except Exception as e:
        logger.error(e)
        if tmp_file.exists():
            tmp_file.unlink(missing_ok=True)
        raise
