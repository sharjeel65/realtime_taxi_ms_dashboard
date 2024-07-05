from pyflink.datastream import TimeCharacteristic
from pyflink.datastream.functions import KeySelector, KeyedProcessFunction
from datetime import datetime
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer
from pyflink.datastream.stream_execution_environment import StreamExecutionEnvironment
from pyflink.common.serialization import SimpleStringSchema


class TaxiKeySelector(KeySelector):
    def get_key(self, value):
        fields = value.split(',')
        return fields[0]  # taxi_id


class ProcessTaxiData(KeyedProcessFunction):
    def process_element(self, value, ctx):
        def __init__(self):
            self.last_location = {}

        def process_element(self, value, ctx):
            fields = value.split(',')
            if len(fields) == 4:
                taxi_id = fields[0]
                timestamp_str = fields[1]
                longitude = float(fields[2])
                latitude = float(fields[3])

                # Convert timestamp to datetime object
                timestamp = datetime.strptime(timestamp_str, "%Y-%m-%d %H:%M:%S")

                # Retrieve last location from state
                if taxi_id in self.last_location:
                    last_timestamp, last_longitude, last_latitude = self.last_location[taxi_id]

                    # Calculate distance using Haversine formula
                    distance = self.calculate_distance(last_longitude, last_latitude, longitude, latitude)

                    # Calculate time difference in seconds
                    time_diff = (timestamp - last_timestamp).total_seconds()

                    if time_diff > 0:
                        # Calculate speed (distance / time)
                        speed = distance / time_diff  # in meters per second

                        # Create a dictionary to store processed data
                        processed_data = {
                            'taxi_id': taxi_id,
                            'timestamp': timestamp,
                            'longitude': longitude,
                            'latitude': latitude,
                            'speed': speed
                        }

                        # Emit the result to downstream operator or sink
                        print("Processed data:", processed_data)

                # Update last location in state
                self.last_location[taxi_id] = (timestamp, longitude, latitude)

        def calculate_distance(self, lon1, lat1, lon2, lat2):
            # Calculate distance between two geographical coordinates using Haversine formula
            # Radius of the Earth in meters
            R = 6371000.0

            # Convert latitude and longitude from degrees to radians
            lat1_rad = radians(lat1)
            lon1_rad = radians(lon1)
            lat2_rad = radians(lat2)
            lon2_rad = radians(lon2)

            # Calculate differences
            dlon = lon2_rad - lon1_rad
            dlat = lat2_rad - lat1_rad

            # Haversine formula
            a = sin(dlat / 2) ** 2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon / 2) ** 2
            c = 2 * atan2(sqrt(a), sqrt(1 - a))
            distance = R * c

            return distance

def kafka_to_flink(topic, group_id="my_consumer_group", bootstrap_servers="localhost:9092"):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)
    env.add_jars("file:///Users/mouni/PycharmProjects/bd24_project_a6_b/flink-connector-kafka-3.2.0-1.19.jar",
                 "file:///Users/mouni/PycharmProjects/bd24_project_a6_b/kafka-clients-3.7.0.jar")
    env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

    kafka_properties = {
        'bootstrap.servers': bootstrap_servers,
        'group.id': group_id,
        'key.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer',
        'value.deserializer': 'org.apache.kafka.common.serialization.StringDeserializer'
    }

    kafka_consumer = FlinkKafkaConsumer(
        topic,
        SimpleStringSchema(),
        properties=kafka_properties
    )

    stream = env.add_source(kafka_consumer)

    stream.key_by(TaxiKeySelector()) \
        .process(ProcessTaxiData())

    env.execute('Flink Kafka Processing')


if __name__ == "__main__":
    kafka_to_flink("taxi12", "my_consumer_group", "localhost:9092")