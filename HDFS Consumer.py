from kafka import KafkaConsumer
import json
from datetime import datetime
import re
import pyarrow as pa
import pandas as pd

from ConfigurationFile import ConfigurationFile

conf=ConfigurationFile()

#hdfs_stg_dir = '/tmp/staging/iics/raw'
hdfs_stg_dir=conf.data['hdfs_stg_dir']
#host = 'localhost'
hdfsHost=conf.data['hdfsHost']
#topic = 'Informatica-Data'
topic=conf.data['topic']

topic2 = 'mapName'
#brokers = ['cnt7-naya-cdh63:9092']
brokers=[conf.data['brokers']]

# connector to hdfs
fs = pa.hdfs.connect(
    host=hdfsHost,
    port=8020,
    user=conf.data["hdfsUser"],
    kerb_ticket=None,
    # driver='libhdfs',
    extra_conf=None)

# Set the consumer
consumer = KafkaConsumer(
    topic,
    group_id='Informatica_HDFS',
    bootstrap_servers=brokers,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)
    #value_deserializer=lambda m: json.loads(m.decode('utf-8')))


# if not exist staging change it
if fs.exists(hdfs_stg_dir):
    fs.rm(hdfs_stg_dir, recursive=True)
    fs.mkdir(hdfs_stg_dir)
else:
    fs.mkdir(hdfs_stg_dir)

for message1 in consumer:


    try:
        events = json.loads(message1.value)
        string = ' '.join(str(item) for item in events)
        print( events)
        data=df = pd.json_normalize(json.loads(events))
        print(f'reading {data} from Producer')
        hdfs_file_name =data["mapName"].iloc[0]
        #print(hdfs_file_name)
        ff = open((hdfs_file_name).strip(), 'w')
        ff.write(str(events))
        ff.flush()
        print("putting files into {}/{}.json" , hdfs_stg_dir , hdfs_file_name)
        with open(str(hdfs_file_name).strip(), 'rb') as ff:
            fs.upload('hdfs://cnt7-naya-cdh63:8020{}/{}.json'.format(hdfs_stg_dir, hdfs_file_name), ff)
    except Exception as e:
            print(e.__traceback__)

