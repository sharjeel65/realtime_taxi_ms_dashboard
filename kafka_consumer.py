from pyflink.datastream import TimeCharacteristic
from pyflink.datastream.functions import ProcessFunction, KeySelector, KeyedProcessFunction
from elasticsearch import Elasticsearch
from datetime import datetime, timedelta
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer, FlinkKafkaConsumer
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.common import SimpleStringSchema, WatermarkStrategy, Types


class TaxiKeySelector(KeySelector):
    def get_key(self, value):
        fields = value.split(',')
        return fields[0]  # taxi_id


# class SimpleKeyedProcessFunction(KeyedProcessFunction):
#     def process_element(self, value, ctx):
#         # Print the incoming string
#         print(value)
#         # No output to collector in this simple example

class ProcessTaxiData(KeyedProcessFunction):
    def process_element(self, value, ctx):
        if not hasattr(self, 'es_client'):
            self.es_client = Elasticsearch(['http://localhost:9200'])
            mapping = {
                "mappings": {
                    "properties": {
                        "taxi_id": {"type": "keyword"},
                        "timestamp": {"type": "date"},
                        "location": {
                            "type": "geo_point"
                        }
                    }
                }
            }
            self.es_client.indices.create(index="taxi_index", body=mapping, ignore=400)
        fields = value.split(',')
        try:
            if len(fields) == 4:
                taxi_id = fields[0]
                timestamp_str = fields[1]
                longitude = float(fields[2])
                latitude = float(fields[3])

                # Convert timestamp to datetime object
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

                # Create document to index in Elasticsearch
                document = {
                    'taxi_id': taxi_id,
                    'timestamp': timestamp,
                    'location': {
                        'lat': latitude,
                        'lon': longitude
                    }
                }
                # Index document into Elasticsearch
                self.es_client.index(index='taxi_index', body=document)
                print("streamed")

        except (ValueError, Exception) as e:
            print(f"Error processing message: {value}, Error: {e}")


def kafka_to_elasticsearch(topic, group_id="my_consumer_group", bootstrap_servers="localhost:9094"):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    # C:\Users\sharj\PycharmProjects\bd24_project_a6_b\flink - connector - kafka - 3.2
    # .0 - 1.19.jar
    # C:\Users\sharj\PycharmProjects\bd24_project_a6_b\flink - connector - kafka - 3.2
    # .0 - 1.19.jar
    env.add_jars("file:///Users/sharj/PycharmProjects/bd24_project_a6_b/flink-connector-kafka-3.2.0-1.19.jar",
                 "file:///Users/sharj/PycharmProjects/bd24_project_a6_b/kafka-clients-3.7.0.jar")
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
    # Create an Elasticsearch client
    es = Elasticsearch(['http://localhost:9200'])

    # Define Kafka properties
    kafka_properties = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'key.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer',
        'value.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer'
    }

    # Create a FlinkKafkaConsumer
    kafka_consumer = FlinkKafkaConsumer(
        topic,
        SimpleStringSchema(),
        properties=kafka_properties
    )

    # Add the Kafka source to the environment
    stream = env.add_source(kafka_consumer)

    # # Define the processing logic
    stream.key_by(TaxiKeySelector()) \
        .process(ProcessTaxiData(), output_type=Types.STRING())
    # Apply KeyedProcessFunction
    # stream.key_by(lambda x: x) \
    #     .process(SimpleKeyedProcessFunction(), output_type=Types.STRING())

    # Execute the Flink job
    env.execute('Flink Kafka to Elasticsearch')


if __name__ == "__main__":
    kafka_to_elasticsearch("taxi", "my_consumer_group", "localhost:9094")
