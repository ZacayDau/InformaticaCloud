import requests
import json
import sys
import random



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


