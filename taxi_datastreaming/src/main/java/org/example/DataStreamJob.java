import Deserializer.JSONValueDeserializationSchema;
import Dto.Transaction;
import bd24_project_a6_b.taxi_datastreaming.src.main.java.Dto.AverageSpeedDistance;

import Dto.TaxiData;
import Dto.AverageSpeedDistance;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.elasticsearch.sink.Elasticsearch7SinkBuilder;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcStatementBuilder;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.elasticsearch7.shaded.org.apache.http.HttpHost;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.action.index.IndexRequest;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.client.Requests;
import org.apache.flink.elasticsearch7.shaded.org.elasticsearch.common.xcontent.XContentType;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.api.common.functions.MapFunction;

import java.sql.Date;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class DataStreamJob {
	private static final String jdbcUrl = "jdbc:postgresql://localhost:5432/postgres";
	private static final String username = "postgres";
	private static final String password = "postgres";

	public static void main(String[] args) throws Exception {
		// Sets up the execution environment, which is the main entry point
		// to building Flink applications.
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("group.id", "flink-group");

		FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>("taxi_data", new SimpleStringSchema(), properties);
		consumer.setStartFromEarliest();

		DataStream<String> rawTaxiDataStream = env.addSource(consumer);

		DataStream<Transaction> taxiDataStream = rawTaxiDataStream.map((MapFunction<String, Transaction>) value -> {
			String[] fields = value.split(",");
			Transaction taxiData = new Transaction();
			taxiData.setTaxiID(fields[0]);
			taxiData.setTimestamp(Timestamp.valueOf(fields[1]));
			taxiData.setLongitude(Double.parseDouble(fields[2]));
			taxiData.setLatitude(Double.parseDouble(fields[3]));
			taxiData.setStarting(Boolean.parseBoolean(fields[4]));
			taxiData.setEnding(Boolean.parseBoolean(fields[5]));
			return taxiData;
		});

		taxiDataStream.print();

		JdbcExecutionOptions execOptions = new JdbcExecutionOptions.Builder()
				.withBatchSize(1000)
				.withBatchIntervalMs(200)
				.withMaxRetries(5)
				.build();

		JdbcConnectionOptions connOptions = new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
				.withUrl(jdbcUrl)
				.withDriverName("org.postgresql.Driver")
				.withUsername(username)
				.withPassword(password)
				.build();

		// Create taxi_data table sink
		taxiDataStream.addSink(JdbcSink.sink(
				"CREATE TABLE IF NOT EXISTS taxi_data (" +
						"taxi_id VARCHAR(255), " +
						"timestamp TIMESTAMP, " +
						"longitude DOUBLE PRECISION, " +
						"latitude DOUBLE PRECISION, " +
						"is_starting BOOLEAN, " +
						"is_ending BOOLEAN, " +
						"PRIMARY KEY (taxi_id, timestamp)" +
						")",
				(JdbcStatementBuilder<Transaction>) (preparedStatement, taxiData) -> {
				},
				execOptions,
				connOptions
		)).name("Create Taxi Data Table Sink");

		// Insert into taxi_data table sink
		taxiDataStream.addSink(JdbcSink.sink(
				"INSERT INTO taxi_data(taxi_id, timestamp, latitude, longitude, is_starting, is_ending) " +
						"VALUES (?, ?, ?, ?, ?, ?) " +
						"ON CONFLICT (taxi_id, timestamp) DO UPDATE SET " +
						"longitude = EXCLUDED.longitude, " +
						"latitude = EXCLUDED.latitude, " +
						"is_starting = EXCLUDED.is_starting, " +
						"is_ending = EXCLUDED.is_ending " +
						"WHERE taxi_data.taxi_id = EXCLUDED.taxi_id " +
						"AND taxi_data.timestamp = EXCLUDED.timestamp",
				(JdbcStatementBuilder<Transaction>) (preparedStatement, taxiData) -> {
					preparedStatement.setString(1, taxiData.getTaxiID());
					preparedStatement.setTimestamp(2, taxiData.getTimestamp());
					preparedStatement.setDouble(3, taxiData.getLongitude());
					preparedStatement.setDouble(4, taxiData.getLatitude());
					preparedStatement.setBoolean(5, taxiData.isStarting());
					preparedStatement.setBoolean(6, taxiData.isEnding());
				},
				execOptions,
				connOptions
		)).name("Insert into taxi data table sink");

		// Calculate average speed and distance, then insert into database
		DataStream<AverageSpeedDistance> avgSpeedDistanceStream = taxiDataStream
				.keyBy(Transaction::getTaxiID)
				.process(new SpeedDistanceCalculator());

		avgSpeedDistanceStream.addSink(JdbcSink.sink(
				"INSERT INTO average_speed_distance(taxi_id, trip_distance, average_speed) " +
						"VALUES (?, ?, ?) " +
						"ON CONFLICT (taxi_id) DO UPDATE SET " +
						"trip_distance = EXCLUDED.trip_distance, " +
						"average_speed = EXCLUDED.average_speed " +
						"WHERE average_speed_distance.taxi_id = EXCLUDED.taxi_id",
				(JdbcStatementBuilder<AverageSpeedDistance>) (preparedStatement, avgSpeedDistance) -> {
					preparedStatement.setString(1, avgSpeedDistance.getTaxiID());
					preparedStatement.setDouble(2, avgSpeedDistance.getTripDistance());
					preparedStatement.setDouble(3, avgSpeedDistance.getAverageSpeed());
				},
				execOptions,
				connOptions
		)).name("Insert into average speed distance table sink");

		// Sink to Elasticsearch
		avgSpeedDistanceStream.sinkTo(
				new Elasticsearch7SinkBuilder<AverageSpeedDistance>()
						.setHosts(new HttpHost("localhost", 9200, "http"))
						.setEmitter((avgSpeedDistance, runtimeContext, requestIndexer) -> {
							String json = String.format("{\"taxiId\":\"%s\",\"tripDistance\":%.2f,\"averageSpeed\":%.2f}",
									avgSpeedDistance.getTaxiID(), avgSpeedDistance.getTripDistance(), avgSpeedDistance.getAverageSpeed());

							IndexRequest indexRequest = Requests.indexRequest()
									.index("average_speed_distance")
									.id(avgSpeedDistance.getTaxiID())
									.source(json, XContentType.JSON);
							requestIndexer.add(indexRequest);
						})
						.build()
		).name("Elasticsearch Sink");

		// Execute program, beginning computation.
		env.execute("Flink Taxi Data Streaming");
	}
}