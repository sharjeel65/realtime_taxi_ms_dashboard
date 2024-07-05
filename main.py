from kafka_producer import produce_taxi_data
from kafka_consumer import consume_data

if __name__ == "__main__":
    taxi_file = "all_sorted_data.csv"
    # Produce data to Kafka topic
    produce_taxi_data(taxi_file)
    print("consumr started")
    # Consume data from Kafka topic
    consume_data()

