from confluent_kafka import Producer
from time import sleep
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TaxiDataProducer")


def delivery_report(err, msg):
    """Delivery report callback called once for each message produced."""
    if err is not None:
        logger.error(f"Delivery failed for message {msg.key()}: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")


def produce_taxi_data(file_path='', batch_size=1, sleep_time=1):
    # Configure Kafka producer
    conf = {
        'bootstrap.servers': "localhost:9094",
        'client.id': 'taxi',
        'linger.ms': 10,
        'batch.num.messages': batch_size
    }
    producer = Producer(**conf)

    # Define topic
    topic = "taxi"
    logger.info("Producer started")

    try:
        with open(file_path, "r") as file:
            logger.info(f"Reading data from {file_path}")
            lines = file.readlines()
            # Loop continuously, reading and sending data in batches
            while True:
                for i in range(0, len(lines), batch_size):
                    batch = lines[i:i + batch_size]
                    for line in batch:
                        producer.produce(topic, line.encode('utf-8'), callback=delivery_report)

                    # Wait for delivery confirmations (optional)
                    producer.poll(20)  # Serve delivery callback queue
                    # producer.flush()  # Ensure all messages are sent before sleeping

                    logger.info(f"Produced and flushed batch of {len(batch)} records")

                # Sleep before the next iteration
                logger.info(f"Sleeping for {sleep_time} seconds before next iteration")
                sleep(sleep_time)

    except Exception as e:
        logger.error(f"Error producing taxi data: {e}")
    finally:
        # Ensure all remaining messages are delivered before exiting
        producer.flush()
        logger.info("Producer ended")


if __name__ == "__main__":
    produce_taxi_data("all_sorted_data.csv", 10, 1)
