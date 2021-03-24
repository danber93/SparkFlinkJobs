import com.datastax.spark.connector.japi.CassandraJavaUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;

import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.dstream.DStream;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import scala.Tuple2;
import scala.Tuple8;

import javax.xml.crypto.Data;
import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;

import static com.datastax.spark.connector.japi.CassandraStreamingJavaUtil.javaFunctions;

public class SSJob1 implements Serializable {

	public static void main(String[] args) throws Exception {


		Map<String, Object> kafkaProperties = new HashMap<String, Object>();
		kafkaProperties.put("bootstrap.servers", "kafka-iot:9092");
		kafkaProperties.put("key.deserializer", StringDeserializer.class);
		kafkaProperties.put("value.deserializer", StringDeserializer.class);
		kafkaProperties.put("group.id", "plaintext");

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("stream1");
		conf.set("spark.cassandra.connection.host", "172.22.0.6");
		conf.set("spark.cassandra.auth.username", "cassandra");
		conf.set("spark.cassandra.auth.password", "cassandra");
		conf.set("spark.cassandra.connection.keep_alive_ms", "100000000");

		//batch interval of 1 seconds for incoming stream
		JavaStreamingContext jssc = new JavaStreamingContext(conf, Durations.milliseconds(1000));
		jssc.sparkContext().setLogLevel("ERROR");

		System.out.println("-----------------------------AFTER JSSC IS SET");

		Collection<String> topics = Arrays.asList("taxi_trip");

		final JavaInputDStream<ConsumerRecord<String, String>> stream =
				KafkaUtils.createDirectStream(
						jssc,
						LocationStrategies.PreferConsistent(),
						ConsumerStrategies.<String, String>Subscribe(topics, kafkaProperties)
						);

		JavaPairDStream<String, String> line = stream.mapToPair(record -> new Tuple2<>(record.key(), record.value()));
		
		JavaDStream<DataRecord> dataRecord = line.map(value -> new DataRecord(
				value._2.split(",")[0], 
				value._2.split(",")[1], 
				value._2.split(",")[4], 
				value._2.split(",")[5], 
				value._2.split(",")[6], 
				value._2.split(",")[7], 
				value._2.split(",")[8].replace("[", "").replace("]", "").replace("\"", " "),
				value._2.split(",")[9].replace("[", "").replace("]", "").replace("\"", " ")
				));

		line.print();


		Map<String, String> columnNameMappingsStream = new HashMap<>();
		columnNameMappingsStream.put("trip_id", "trip_id");
		columnNameMappingsStream.put("call_id", "call_id");
		columnNameMappingsStream.put("taxi_id", "taxi_id");
		columnNameMappingsStream.put("timestamp_trip_id", "timestamp_trip_id");
		columnNameMappingsStream.put("day_id", "day_id");
		columnNameMappingsStream.put("missing_data", "missing_data");
		columnNameMappingsStream.put("lon", "lon");
		columnNameMappingsStream.put("lat", "lat");

		// call CassandraStreamingJavaUtil function to save in DB
		javaFunctions(dataRecord).writerBuilder(
				"stream_spark",
				"stream_1",
				CassandraJavaUtil.mapToRow(DataRecord.class, columnNameMappingsStream)
				).saveToCassandra();

		jssc.start();
		jssc.awaitTermination();
	}
}