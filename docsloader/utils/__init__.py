import importlib
from typing import TYPE_CHECKING

__all__ = [
    'download_to_tmpfile',
    'format_image',
    'format_table',
    'is_empty_file',
    'mk_tmpdir',
    'office_cvt_openxml',
    'rm_dir',
    'rm_empty_dir',
    'rm_file',
]

if TYPE_CHECKING:
    from docsloader.utils._download_to_tmpfile import download_to_tmpfile
    from docsloader.utils._format_image import format_image
    from docsloader.utils._format_table import format_table
    from docsloader.utils._is_empty_file import is_empty_file
    from docsloader.utils._mk_tmpdir import mk_tmpdir
    from docsloader.utils._office_cvt_openxml import office_cvt_openxml
    from docsloader.utils._rm_dir import rm_dir
    from docsloader.utils._rm_empty_dir import rm_empty_dir
    from docsloader.utils._rm_file import rm_file


def __getattr__(name):
    if name in __all__:
        module = importlib.import_module(f"docsloader.utils._{name}")
        return getattr(module, name)
    raise AttributeError(f"ImportError: cannot import name '{name}' from 'docsloader.utils'")
