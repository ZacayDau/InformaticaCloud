#
from kafka import KafkaConsumer
import json
from datetime import datetime

from elasticsearch import Elasticsearch

from ConfigurationFile import ConfigurationFile

cn=ConfigurationFile()

topicLogs = 'Logs'
brokers = [cn.data["brokers"] ]

# Set the consumer
consumer = KafkaConsumer(
    topicLogs,
    group_id='Informatica_HDFS',
    bootstrap_servers=brokers,
    auto_offset_reset='latest',
    enable_auto_commit=True,
    auto_commit_interval_ms=1000)
#es = Elasticsearch(http_compress=True)

es = Elasticsearch(cn.data["elasticHost"] )

for message1 in consumer:
    events = json.loads(message1.value)
    elasticOpaqueId = 'python-eland-requests'
    elasticScheme = 'http'
    elasticPort = 9200
    elasticTimeout = 100
    print(events)
    es.index(index="new-index", document={"log": events, "timestamp": datetime.now()})