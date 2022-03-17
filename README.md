# solita-kafka

Pushing GitHub repo languages from GitHub API to Confluent Kafka.

Usage:
```
virtualenv venv
source ./venv/bin/activate
pip install -r requirements.txt
./github_producer.py -p YOUR_GITHUB_PERSONAL_ACCESS_TOKEN -f ~/.confluent/python.config -t topic-name
```
