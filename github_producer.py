#!/usr/bin/env python

from os import access
import sys
from github import Github
from confluent_kafka import Producer, KafkaError
import ccloud_lib

args = ccloud_lib.parse_args()
config_file = args.config_file
topic = args.topic
access_token = args.pat
conf = ccloud_lib.read_ccloud_config(config_file)

g = Github(access_token)

producer_conf = ccloud_lib.pop_schema_registry_params_from_config(conf)
producer = Producer(producer_conf)

ccloud_lib.create_topic(conf, topic)

delivered_records = 0


def acked(err, msg):
    global delivered_records
    if err is not None:
        print("Failed to deliver message: {}".format(err))
    else:
        delivered_records += 1
        print("Produced record to topic {} partition [{}] @ offset {}"
              .format(msg.topic(), msg.partition(), msg.offset()))


for repo in g.get_repos():
    producer.produce(topic, key=repo.name,
                     value=str(repo.get_languages()), on_delivery=acked)
    producer.poll(0)
