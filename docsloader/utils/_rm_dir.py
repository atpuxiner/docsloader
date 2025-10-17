import os
import shutil


def rm_dir(dirpath: str):
    """删除目录"""
    if dirpath and os.path.isdir(dirpath):
        shutil.rmtree(dirpath)
