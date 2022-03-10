# digitraffic-kafka

Pushing AIS messages from Digitraffic marine MQTT to Confluent Kafka.

Usage:
```
virtualenv venv
source ./venv/bin/activate
pip install -r requirements.txt
./producer_dt.py -f ~/.confluent/python.config -t topic-name
```
