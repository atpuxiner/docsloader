import tempfile


def mk_tmpdir() -> str:
    """创建临时目录"""
    return tempfile.mkdtemp()
