from confluent_kafka import Consumer, KafkaError


def consume_data():
    # Configure Kafka consumer
    conf = {'bootstrap.servers': "localhost:9092", 'group.id': "my_consumer_group", 'auto.offset.reset': 'earliest'}
    consumer = Consumer(conf)

    # Subscribe to topic
    topic = "taxi_1"
    consumer.subscribe([topic])

    try:
        # Consume messages from Kafka topic
        while True:
            msg = consumer.poll(timeout=1.0)
            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError.PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    # Handle other errors
                    print(f"Consumer error: {msg.error()}")
                    break

            # Process the message
            print(f'Received message: {msg.value().decode("utf-8")}')
    except Exception as e:
        print(f"Error consuming messages: {e}")
    finally:
        # Close consumer
        consumer.close()
        print("Consumer closed.")


if __name__ == "__main__":
    consume_data()
