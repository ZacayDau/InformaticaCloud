from os.path import exists
import json

class ConfigurationFile:

    def __init__(self):
        self.path="/home/naya/downloads/iics/config.json"
        file_exists = exists(self.path)
        if file_exists:
            with open(self.path) as json_file:
                self.data = json.load(json_file)
