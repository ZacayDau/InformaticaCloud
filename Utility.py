import json
import logging
import zipfile
import shutil
from datetime import datetime
from pathlib import Path

from ConfigurationFile import ConfigurationFile

cn=ConfigurationFile()
WORKINGDIR=cn.data["WORKINGDIR"]
log_dir =cn.data["log_dir"]
timestampNow = datetime.now()

class JSONFormatter(logging.Formatter):
   def __init__(self):
      super().__init__()
   def format(self, record):
      record.msg = json.dumps(record.msg)
      return super().format(record)

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
loggingStreamHandler = logging.StreamHandler()
loggingStreamHandler = logging.FileHandler(log_dir+"/"+"logs.json",mode='a') #to save to file
loggingStreamHandler.setFormatter(JSONFormatter())
logger.addHandler(loggingStreamHandler)


def unzipFile(path_to_zip_file,directory_to_extract_to):
    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:

        try:
            zip_ref.extractall(directory_to_extract_to)
        except Exception as e:
            print(e.__traceback__)
            logger.error({"date": str(timestampNow), "source": "Utility", "data": e.__traceback__})


def copyFile(src,dst):
    try:
        shutil.copyfile(src, dst)
    except Exception as e:
        print(e.__traceback__)
        logger.error({"date": str(timestampNow), "source": "Utility", "data": e.__traceback__})

def moveFile(src,tgt):
    try:
        shutil.move(src, tgt)
    except Exception as e:
        print(e.__traceback__)
        logger.error({"date": str(timestampNow), "source": "Utility", "data": e.__traceback__})

def mkDir(src):
    try:
        Path(src).mkdir(parents=True, exist_ok=True)
    except Exception as e:
        print(e.__traceback__)
        logger.error({"date": str(timestampNow), "source": "Utility", "data": e.__traceback__})