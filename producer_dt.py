#!/usr/bin/env python

from confluent_kafka import Producer, KafkaError
import json
import ccloud_lib
import uuid
import paho.mqtt.client as mqtt
import time

APP_NAME = 'KafkaTest'

args = ccloud_lib.parse_args()
config_file = args.config_file
topic = args.topic
conf = ccloud_lib.read_ccloud_config(config_file)

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

def on_message(client, userdata, message):
    decoded_message = message.payload.decode('utf-8')
    payload = json.loads(decoded_message)
    if 'mmsi' in payload:
        record_key = payload['mmsi']
        producer.produce(topic, key=str(record_key), value=decoded_message, on_delivery=acked)
        producer.poll(0)

def on_connect(client, userdata, flags, rc):
    if rc == 0:
        print('Connected')
        client.subscribe("vessels/#")
    else:
        print('Failed to connect, return code %d\n', rc)

client_name = '{}; {}'.format(APP_NAME, str(uuid.uuid4()))
client = mqtt.Client(client_name, transport="websockets")

client.username_pw_set('digitraffic', 'digitrafficPassword')
client.on_connect = on_connect
client.on_message = on_message

client.tls_set()
client.connect('meri.digitraffic.fi', 61619)

client.loop_forever()
