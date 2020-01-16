from kafka  import KafkaProducer
from time import sleep
from json import dumps


def generate_locations_stream(input_location_transaction):

    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'), linger_ms=10, api_version=(2,4,0))

    with open(input_location_transaction,mode='r') as f:
        for line in f:
            json_comb = dumps(line)                                         # pick observation and encode to JSON
            producer.send("locations", value=json_comb)           # send encoded observation to Kafka topic
            sleep(5)

    producer.flush()
    producer.close()


def main():
    input_location_transaction = "C:/Users/Dell/Downloads/input_data/locations.json"
    generate_locations_stream(input_location_transaction)


if __name__ == "__main__":
    main()
