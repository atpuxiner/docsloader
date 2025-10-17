import os


def is_empty_file(file_path) -> bool:
    """是否空文件"""
    return os.path.getsize(file_path) == 0
