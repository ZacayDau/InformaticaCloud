import logging
from datetime import datetime

import requests
import json
import sys
import random

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


class Slack:

    def __init__(self,url):
        self.url =url
        self.url = url


    def sendMessage(self,txt):
        message = (txt)
        title = (f"New Incoming Message :Informatica Cloud:")
        slack_data = {
                "username": "NotificationBot",
                "icon_emoji": ":satellite:",
                # "channel" : "#somerandomcahnnel",
                "attachments": [
                    {
                        "color": "#9733EE",
                        "fields": [
                            {
                                "title": title,
                                "value": message,
                                "short": "false",
                            }
                        ]
                    }
                ]
            }
        byte_length = str(sys.getsizeof(slack_data))
        headers = {'Content-Type': "application/json", 'Content-Length': byte_length}
        response = requests.post(self.url, data=json.dumps(slack_data), headers=headers)
        if response.status_code != 200:
                raise Exception(response.status_code, response.text)
                logger.error({"date": str(timestampNow), "source": "Slack", "data":  response.text})



