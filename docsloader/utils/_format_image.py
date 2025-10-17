import base64
from pathlib import Path
from typing import Literal


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
