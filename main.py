from kafka_producer import produce_data_batch
from kafka_consumer import consume_data

if __name__ == "__main__":
    # Produce data to Kafka topic
    produce_data_batch()
    print("consumr started")
    # Consume data from Kafka topic
    consume_data()
