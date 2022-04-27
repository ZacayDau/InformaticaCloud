import zipfile
import shutil
from pathlib import Path


def unzipFile(path_to_zip_file,directory_to_extract_to):
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:

        try:
            zip_ref.extractall(directory_to_extract_to)
        except Exception as e:
            print(e.__traceback__)

def copyFile(src,dst):
    try:
        shutil.copyfile(src, dst)
    except Exception as e:
        print(e.__traceback__)

def moveFile(src,tgt):
    try:
        shutil.move(src, tgt)
    except Exception as e:
        print(e.__traceback__)

def mkDir(src):
    try:
        Path(src).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(e.__traceback__)