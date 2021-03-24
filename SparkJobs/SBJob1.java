import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;

import com.datastax.spark.connector.japi.CassandraJavaUtil;

import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;
import scala.Tuple3;

import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapRowTo;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

public class SBJob1 {

	public static void main(String[] args) {

		SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("batch");
		conf.set("spark.cassandra.connection.host", "172.22.0.6");
		conf.set("spark.cassandra.auth.username", "cassandra");
		conf.set("spark.cassandra.auth.password", "cassandra");
		conf.set("spark.cassandra.connection.keep_alive_ms", "100000000");


		SparkContext sc = new SparkContext(conf);
		//		sc.setLogLevel("ERROR");

		System.out.println("-----------------------------AFTER SC IS SET");

		JavaRDD<DataRecord> recordRdd = CassandraJavaUtil.javaFunctions(sc)
				.cassandraTable("stream_spark","stream_1", mapRowTo(DataRecord.class));

		System.out.println("-----------------------------LETTURA DA CASSANDRA ESEGUITA");

		JavaRDD<DataRecord> line = recordRdd.filter(record -> record.getMissing_data().equals("False"));

		JavaRDD<Tuple5<String, Long, Double, Double ,String>> five = line.map(record -> new Tuple5<>(record.getTrip_id(), record.getTimestamp_trip_id(), record.getLon(), record.getLat(), record.getTaxi_id()));
		
		JavaPairRDD<String, Iterable<Tuple5<String, Long, Double, Double ,String>>> groupRdd = five.groupBy(tuple -> tuple._1());

		JavaRDD<BatchTripDist> result = groupRdd.map(group -> new BatchTripDist(group._2()));

		// call CassandraJavaUtil function to save in DB
		CassandraJavaUtil.javaFunctions(result)
		.writerBuilder("batch_spark", "batch_1", mapToRow(BatchTripDist.class)).saveToCassandra();

		long count = result.count();

		System.out.println(count);

	}

}
