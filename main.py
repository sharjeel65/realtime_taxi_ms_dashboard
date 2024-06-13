from kafka_producer import produce_taxi_data
from kafka_consumer import consume_data
from threading import Thread
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Main")


def main():
    # Kafka configuration parameters
    kafka_topic = "taxi_1"
    csv_file_path = "all_sorted_data.csv"
    bootstrap_servers = "localhost:9092"
    consumer_group_id = "my_consumer_group"
    batch_size = 10  # Number of records per batch
    sleep_time = 1  # Time to sleep between batches
    poll_timeout = 1.0  # Consumer poll timeout

    # Producer thread
    producer_thread = Thread(target=produce_taxi_data, args=(csv_file_path, batch_size, sleep_time))
    # Consumer thread
    consumer_thread = Thread(target=consume_data,
                             args=(kafka_topic, consumer_group_id, bootstrap_servers, poll_timeout))

    logger.info("Starting producer and consumer threads")
    producer_thread.start()
    consumer_thread.start()

    producer_thread.join()
    consumer_thread.join()
    # consume_data(topic=kafka_topic, group_id=consumer_group_id, bootstrap_servers=bootstrap_servers,
    #              timeout=poll_timeout)
    # produce_taxi_data(file_path=csv_file_path, batch_size=batch_size, sleep_time=sleep_time)

    logger.info("Producer and consumer threads have finished")


if __name__ == "__main__":
    main()
