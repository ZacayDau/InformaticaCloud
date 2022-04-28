import logging

from kafka import KafkaConsumer
import json
from datetime import datetime
import re
import pyarrow as pa
import pandas as pd

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


#hdfs_stg_dir = '/tmp/staging/iics/raw'
hdfs_stg_dir=cn.data['hdfs_stg_dir']
#host = 'localhost'
hdfsHost=cn.data['hdfsHost']
#topic = 'Informatica-Data'
topic=cn.data['topic']

topic2 = 'mapName'
#brokers = ['cnt7-naya-cdh63:9092']
brokers=[cn.data['brokers']]

# connector to hdfs
fs = pa.hdfs.connect(
    host=hdfsHost,
    port=8020,
    user=cn.data["hdfsUser"],
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
        logger.info({"date": str(timestampNow), "source": "HDFS-Consumer", "data":  events})

        data=df = pd.json_normalize(json.loads(events))
        print(f'reading {data} from Producer')
        logger.info({"date": str(timestampNow), "source": "HDFS-Consumer", "data": "reading {} from Producer".format(data)   })

        hdfs_file_name =data["mapName"].iloc[0]
        #print(hdfs_file_name)
        ff = open((hdfs_file_name).strip(), 'w')
        ff.write(str(events))
        ff.flush()
        print("putting files into {}/{}.json" , hdfs_stg_dir , hdfs_file_name)
        logger.info({"date": str(timestampNow), "source": "HDFS-Consumer", "data":
            "putting files into {}/{}.json".format(hdfs_stg_dir,hdfs_file_name)})

        with open(str(hdfs_file_name).strip(), 'rb') as ff:
            fs.upload('hdfs://cnt7-naya-cdh63:8020{}/{}.json'.format(hdfs_stg_dir, hdfs_file_name), ff)
    except Exception as e:
            print(e.__traceback__)
            logger.error({"date": str(timestampNow), "source": "HDFS-Consumer", "data": e.__traceback__})

