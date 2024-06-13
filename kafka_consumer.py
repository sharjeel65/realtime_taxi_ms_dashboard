from confluent_kafka import Consumer, KafkaError, KafkaException
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TaxiDataConsumer")


def consume_data(topic, group_id="my_consumer_group", bootstrap_servers="localhost:9092", timeout=1.0):
    # Configure Kafka consumer
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
    }
    consumer = Consumer(conf)

    # Subscribe to topic
    consumer.subscribe([topic])
    logger.info(f"Subscribed to topic: {topic}")

    try:
        # Consume messages from Kafka topic
        while True:
            msg = consumer.poll(timeout=timeout)
            if msg is None:
                print("continue")
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.info(f"Reached end of partition: {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")
                    continue
                else:
                    # Handle other errors
                    logger.error(f"Consumer error: {msg.error()}")
                    continue

            # Process the message
            logger.info(f"Received message: {msg.value().decode('utf-8')}")

    except KafkaException as e:
        logger.error(f"Kafka error: {e}")
    except Exception as e:
        logger.error(f"Error consuming messages: {e}")
    finally:
        # Close consumer
        consumer.close()
        logger.info("Consumer closed.")


if __name__ == "__main__":
    consume_data(topic="taxi_1")
