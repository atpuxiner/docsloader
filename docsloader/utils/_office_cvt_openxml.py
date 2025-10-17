import logging
import os
import platform
import shutil
import tempfile
import time

_system_name = platform.system().lower()
if _system_name.startswith("win"):
    import win32com.client as win32_client
else:
    import subprocess

logger = logging.getLogger(__name__)


def office_cvt_openxml(filepath: str, file_suffix: str = None) -> str:
    """
    转换到新的文档格式
    - .xls > .xlsx
    - .ppt > .pptx
    - .doc > .docx
    """
    suffix_map = {
        ".xls": ".xlsx",
        ".ppt": ".pptx",
        ".doc": ".docx",
    }
    file_suffix = file_suffix or os.path.splitext(filepath)[-1].lower()
    to_suffix = suffix_map.get(file_suffix)
    if not to_suffix:
        logger.warning(f"只支持转换 .xls .ppt .doc，否则返回原文件")
        return filepath
    logger.info(f"正在转换 {file_suffix} > {to_suffix} 文件...")
    file_format_id = {
        ".xlsx": 51,
        ".pptx": 24,
        ".docx": 16,
    }[to_suffix]
    if _system_name.startswith("win"):
        with tempfile.NamedTemporaryFile(suffix=to_suffix, delete=False) as tmp_file:
            cvt_filepath = tmp_file.name
        app_info = {
            ".xls": {
                "dispatch_names": ("Ket.Application", "et.Application", "Excel.Application"),
                "worker_name": "Workbooks",
            },
            ".ppt": {
                "dispatch_names": ("Kwpp.Application", "wpp.Application", "PowerPoint.Application"),
                "worker_name": "Presentations",
            },
            ".doc": {
                "dispatch_names": ("Kwps.Application", "wps.Application", "Word.Application"),
                "worker_name": "Documents",
            },
        }
        config = app_info[file_suffix]
        app = None
        for dispatch_name in config["dispatch_names"]:
            try:
                app = win32_client.Dispatch(dispatch_name)
                break
            except Exception as e:
                logger.error(f"尝试 {dispatch_name} 失败: {e}")
                if app is not None:
                    try:
                        app.Quit()
                    except:
                        pass
                app = None
        if not app:
            if cvt_filepath and os.path.isfile(cvt_filepath):
                os.remove(cvt_filepath)
            raise EnvironmentError(f"无法识别转换应用，请安装 MS Office 或 WPS Office")
        try:
            worker = getattr(app, config["worker_name"])
            doc = worker.Open(filepath)
            time.sleep(0.2)
            doc.SaveAs(cvt_filepath, FileFormat=file_format_id)
            doc.Close()
        finally:
            try:
                app.Quit()
            except:
                pass
    else:
        if not shutil.which("libreoffice"):
            raise EnvironmentError("Cannot find libreoffice command. Please install libreoffice and required fonts.")
        output_dir = os.path.dirname(filepath) or "."
        cmd = [
            "libreoffice",
            "--headless",
            "--convert-to", to_suffix.lstrip("."),
            "--outdir", output_dir,
            filepath
        ]
        subprocess.run(cmd, check=True, capture_output=True)
        cvt_filepath = os.path.splitext(filepath)[0] + to_suffix
    return cvt_filepath
