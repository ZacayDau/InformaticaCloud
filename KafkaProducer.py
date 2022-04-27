from kafka import KafkaProducer
import json
from time import sleep
import os

# Topics/Brokers
import Utility
from ConfigurationFile import ConfigurationFile

conf = ConfigurationFile()

#topic = 'Informatica-Data'
topic=conf.data["topic"]
#brokers = ['cnt7-naya-cdh63:9092']
brokers=[conf.data["brokers"]]

#source_file = '/home/naya/downloads/iics/rawData/'
source_file=conf.data["source_file"]
#histDir = '/home/naya/downloads/iics/rawData/hist'
histDir=conf.data["histDir"]


# First we set the producer.
producer = KafkaProducer(
    bootstrap_servers = brokers,
    client_id = 'producer',
    acks = 1,
    compression_type = None,
    retries = 3,
    reconnect_backoff_ms = 50,
    reconnect_backoff_max_ms= 1000)

# Send the data
Utility.mkDir(histDir)

while True:
    for file in  os.listdir(source_file):

        if file.endswith(".json"):
            print(file)

            with open(source_file+"/"+file) as f:
                #while True:
                #jsonObject = json.load(f)
                lines = f.read()  # returns list of strings
                print(lines)
                if not lines:
                    sleep(1)
                    f.seek(f.tell())

                producer.send(topic=topic, value=json.dumps(lines).encode('utf-8'))
                producer.flush()
                Utility.moveFile(source_file + "/" + file, histDir + "/" + file)
    sleep(10)
