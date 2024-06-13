from confluent_kafka import Producer


# bin/zookeeper-server-start.sh config/zookeeper.properties
# .\bin\windows\zookeeper-server-start.bat .\config\zookeeper.properties
# .\bin\windows\kafka - topics.bat - -create - -zookeeper localhost: 2181 - -replication - factor 1 - -partitions 1 - -topic taxi_1
# .\bin\windows\kafka-topics.bat --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic taxi_1
# .\bin\windows\kafka-server-start.bat .\config\server.properties
def produce_data_batch(batch_size=100):
    # Configure Kafka producer
    conf = {'bootstrap.servers': "localhost:9092"}
    producer = Producer(**conf)

    # Define topic
    topic = "taxi6"

    # Read data from source and produce it to Kafka topic in batches
    with open("all_sorted_data.csv", "r") as file:
        print("here")
        lines = file.readlines()
        for i in range(0, len(lines), batch_size):
            #print("here 3")
            batch = lines[i:i + batch_size]
            for line in batch:
                producer.produce(topic, line.encode('utf-8'))

            # Wait for all messages in the batch to be delivered
            producer.flush()

    # Wait for all messages to be delivered
    producer.flush()


if __name__ == "__main__":
    produce_data_batch()
