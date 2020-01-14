from kafka import KafkaProducer
from time import sleep
from json import dumps
import os


def generate_stream(**kwargs):
    input_file_loc = kwargs['path_stream']
    topic = kwargs['Topic']
    producer = KafkaProducer(bootstrap_servers=['kafka:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'), linger_ms=10, api_version=(1,4,6))

    with open(os.getcwd() + input_file_loc,mode='r') as f:
        for line in f:
            json_comb = dumps(line)                         # pick observation and encode to JSON
            producer.send(topic, value=json_comb)           # send encoded observation to Kafka topic
            sleep(2)

    producer.flush()
    producer.close()
