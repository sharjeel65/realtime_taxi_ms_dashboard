from confluent_kafka import Producer
import time

def delivery_report(err, msg):
    """Delivery report callback."""
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}] at offset {msg.offset()}")

def kafka_producer_from_file(topic, input_file, bootstrap_servers='localhost:9092'):
    # Create a Producer instance
    producer = Producer({
        'bootstrap.servers': bootstrap_servers,
        'client.id': 'python-producer'
    })

    try:
        # Read input file line by line
        with open(input_file, 'r') as file:
            for line in file:
                # Produce message to Kafka
                message = line.strip()
                producer.produce(topic, message.encode('utf-8'), callback=delivery_report)
                print(f"Produced message: {message}")
                time.sleep(1)  # Introduce a delay between messages

        # Wait for all messages to be delivered
        producer.flush()

    except FileNotFoundError:
        print(f"Error: File '{input_file}' not found.")

    except Exception as e:
        print(f"Error producing messages: {e}")

    finally:
        # No need to close the producer explicitly
        pass

if __name__ == "__main__":
    # Example usage:
    kafka_producer_from_file("taxi12", "9971.txt")