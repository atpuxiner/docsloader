import os


def rm_empty_dir(dirpath: str):
    """删除空目录"""
    if dirpath and os.path.isdir(dirpath):
        with os.scandir(dirpath) as entries:
            if not next(entries, None):
                os.rmdir(dirpath)
