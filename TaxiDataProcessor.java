import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.JSONKeyValueDeserializationSchema;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import java.util.Properties;

public class TaxiDataProcessor {

    public static void main(String[] args) throws Exception {
        // Set up the execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // Configure Kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9092");
        kafkaProps.setProperty("group.id", "taxi-group");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<>(
                "taxi-input-topic",
                new SimpleStringSchema(),
                kafkaProps
        );

        // Configure Kafka producer
        FlinkKafkaProducer<String> kafkaProducer = new FlinkKafkaProducer<>(
                "taxi-output-topic",
                new SimpleStringSchema(),
                kafkaProps
        );

        // Add Kafka consumer as source
        DataStream<String> inputStream = env.addSource(kafkaConsumer);

        // Transform input data into TaxiRide objects
        DataStream<TaxiRide> taxiRides = inputStream.map(new MapFunction<String, TaxiRide>() {
            @Override
            public TaxiRide map(String value) throws Exception {
                // Assuming input is CSV: taxiId,timestamp,latitude,longitude,speed
                String[] fields = value.split(",");
                return new TaxiRide(fields[0], Long.parseLong(fields[1]), Double.parseDouble(fields[2]),
                        Double.parseDouble(fields[3]), int.parseInt(fields[4],int.parseInt(fields[5]));
            }
        });

        // Calculate average speed and distance for each taxi
        DataStream<String> resultStream = taxiRides
                .keyBy(TaxiRide::getTaxiId)
                .process(new TaxiStatsFunction())
                .map(new MapFunction<TaxiStats, String>() {
                    @Override
                    public String map(TaxiStats value) throws Exception {
                        return String.format("TaxiId: %s, AvgSpeed: %.2f, TotalDistance: %.2f",
                                value.getTaxiId(), value.getAverageSpeed(), value.getTotalDistance());
                    }
                });

        // Send results to Kafka output topic
        resultStream.addSink(kafkaProducer);

        // Execute the Flink job
        env.execute("Taxi Data Processor");
    }

    public static class TaxiRide {
        private String taxiId;
        private long timestamp;
        private double latitude;
        private double longitude;
        private int IsStarting;
        private int IsEnding;

        public TaxiRide(String taxiId, long timestamp, double latitude, double longitude, int IsStarting, int IsEnding) {
            this.taxiId = taxiId;
            this.timestamp = timestamp;
            this.latitude = latitude;
            this.longitude = longitude;
            this.IsStarting = IsStarting;
            this.IsEnding=IsEnding;
        }

        public String getTaxiId() {
            return taxiId;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public double getLatitude() {
            return latitude;
        }

        public double getLongitude() {
            return longitude;
        }

        public int getIsStarting() {
            return IsStarting;
        }
        public int getIsEnding() {
            return IsEnding;
    }

    public static class TaxiStats {
        private String taxiId;
        private double totalSpeed;
        private double totalDistance;
        private long rideCount;

        public TaxiStats() {
        }

        public TaxiStats(String taxiId, double totalSpeed, double totalDistance, long rideCount) {
            this.taxiId = taxiId;
            this.totalSpeed = totalSpeed;
            this.totalDistance = totalDistance;
            this.rideCount = rideCount;
        }

        public String getTaxiId() {
            return taxiId;
        }

        public double getAverageSpeed() {
            return totalSpeed / rideCount;
        }

        public double getTotalDistance() {
            return totalDistance;
        }

        public void addRide(double speed, double distance) {
            totalSpeed += speed;
            totalDistance += distance;
            rideCount++;
        }
    }

    public static class TaxiStatsFunction extends KeyedProcessFunction<String, TaxiRide, TaxiStats> {
        private final TaxiStats taxiStats = new TaxiStats();

        @Override
        public void processElement(TaxiRide value, Context ctx, Collector<TaxiStats> out) throws Exception {
            // Simulating distance calculation (not accurate)
            double distance = Math.sqrt(Math.pow(value.getLatitude(), 2) + Math.pow(value.getLongitude(), 2));
            taxiStats.addRide(value.getSpeed(), distance);
            taxiStats.taxiId = value.getTaxiId();
            out.collect(taxiStats);
        }
    }
}