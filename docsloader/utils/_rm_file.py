import os


def rm_file(filepath: str):
    """删除文件"""
    if filepath and os.path.isfile(filepath):
        os.remove(filepath)
