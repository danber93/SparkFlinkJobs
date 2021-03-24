package flinkJobs;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.cassandra.CassandraSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import java.util.Properties;

public class FSJob1 {

    public static void main(String[] args) throws Exception {

        //final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(new Configuration()).setParallelism(2);
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties props = new Properties();
        //set Kafka and Zookeeper IPs
        props.setProperty("zookeeper.connect", "127.0.0.1:2181");
        //props.setProperty("zookeeper.connect", "93.34.92.220:2181");
        props.setProperty("bootstrap.servers", "127.0.0.1:9092"); //Kafka
        //props.setProperty("bootstrap.servers", "93.34.92.220:9092"); //Kafka
        //set group id, not to be shared with another job consuming the same topic
        props.setProperty("group.id", "flink_streaming_job_1");
        //props.setProperty("auto.offset.reset", "earliest");

        FlinkKafkaConsumer<String> kafkaConsumer = new FlinkKafkaConsumer<String>(
                "taxi_trip", new SimpleStringSchema(), props);


        //TRIP_ID,"CALL_TYPE","ORIGIN_CALL","ORIGIN_STAND","TAXI_ID","TIMESTAMP","DAY_TYPE","MISSING_DATA","POLYLINE"
        //0: TRIP_ID
        //1: "CALL_TYPE"
        //2: "ORIGIN_CALL"
        //3: "ORIGIN_STAND"
        //4: "TAXI_ID"
        //5: "TIMESTAMP"
        //6: "DAY_TYPE"
        //7: "MISSING_DATA"
        //8: "POLYLINE" (ma questo splitta in due!!)

        DataStream<Tuple8<String, String, String, String, Long, Double, Double, String>> single_piece_trp = env.addSource(kafkaConsumer).flatMap(

                new FlatMapFunction<String, Tuple8<String, String, String, String, Long, Double, Double, String>>() {
                    @Override
                    public void flatMap(String s, Collector<Tuple8<String, String, String, String, Long, Double, Double, String>> collector) throws Exception {


                        String value = s.replace("\"", "")
                                .replace("[", "")
                                .replace("]", "");

                        //Will splits POLYLINE into two values!
                        String[] values = value.split(",");

                        /*0: ﻿1372636858620000589
                        1: C
                        2:
                        3:
                        4: 20000589
                        5: 1372636858
                        6: A
                        7: False
                        8: -8.618643
                        9:  41.141412*/

                        //Split the polyline and transform!
                        if (values.length == 10) {

                            Long timestamp = Long.parseLong(values[5]);
                            Double lat = Double.parseDouble(values[8]);
                            Double longitude = Double.parseDouble(values[9]);

                            collector.collect(
                                    //stream_1(trip_id varchar, taxi_id varchar, call_id varchar, day_id varchar,
                                    // timestamp_trip_id varchar, lat bigint, long bigint, missing_data varchar, PRIMARY KEY(trip_id))
                                    new Tuple8<String, String, String, String, Long, Double, Double, String>(values[0], values[4], values[1],
                                            values[6], timestamp, lat, longitude, values[7]));
                        }
                    }
                });

        CassandraSink<Tuple8<String, String, String, String, Long, Double, Double, String>> csb =
                CassandraSink.addSink(single_piece_trp).setHost("localhost", 9042)
                .setQuery("INSERT INTO flink_jobs.stream_1_8(trip_id, taxi_id, call_id, day_id, timestamp_trip_id, lat, longitude, missing_data) values (?, ?, ?, ?, ?, ?, ?, ?);")
                .build();

        /*CassandraSink.addSink(single_piece_trp)
                .setHost("localhost", 9042)
                .setQuery("INSERT INTO flink_jobs.stream_1(trip_id, taxi_id, call_id, day_id, timestamp_trip_id, lat, longitude, missing_data) values (?, ?, ?, ?, ?, ?, ?, ?);")
                .build();*/

        env.execute();
    }
}
