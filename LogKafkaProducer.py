from kafka import KafkaProducer
from time import sleep
import json
# Topics/Brokers
from ConfigurationFile import ConfigurationFile

cn=ConfigurationFile()
topicLogs = 'Logs'
#brokers = ['cnt7-naya-cdh63:9092']
brokers=[cn.data["brokers"]]



WORKINGDIR=cn.data["WORKINGDIR"]
log_dir =cn.data["log_dir"]

source_file_topic3 = log_dir+"/"+"logs.json"

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
with open(source_file_topic3, 'r') as f:
    while True:
        lines = f.readline()  # returns list of strings
        # print(lines)
        if not lines:
            sleep(1)
            f.seek(f.tell())
        else:
            print(lines)
            producer.send(topic=topicLogs, value=json.dumps(lines).encode('utf-8'))
            producer.flush()
            sleep(3)
        # print(f.tell())