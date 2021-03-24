package flinkJobs;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.*;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraTupleOutputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

public class FBJob1 {

    public static void main(String[] args) throws Exception {

        System.out.println("START TIMESTAMP: " + System.currentTimeMillis());

        final ExecutionEnvironment env1 = ExecutionEnvironment.getExecutionEnvironment();
        env1.setParallelism(1);

        ClusterBuilder clusterBuilder = new ClusterBuilder() {

            @Override
            public Cluster buildCluster(Cluster.Builder builder) {

                return builder.addContactPoint("localhost")
                        .withPort(9042)
                        .build();
            }
        };

        DataSet<Tuple8<String, Long, String, String, Double, Double, String, String>> inputRecords = env1
                .createInput
                        (new CassandraInputFormat<Tuple8<String, Long, String, String, Double, Double, String, String>>
                                        ("select trip_id, timestamp_trip_id, call_id, day_id, lat, longitude, missing_data, taxi_id from flink_jobs.stream_1_8",clusterBuilder)
                                , TupleTypeInfo.of(new TypeHint<Tuple8<String, Long, String, String, Double, Double, String, String>>() {})).filter(value -> value.f6.equals("False"));

        //inputRecords.print();

        //trip_id, taxi_id, timestamp, lat, long
        DataSet<Tuple5<String, String, Long, Double, Double>> latNlong = inputRecords.map(value -> {
            return new Tuple5<String, String, Long, Double, Double>(value.f0, value.f7, value.f1, value.f4, value.f5);
        }).returns(new TypeHint<Tuple5<String, String, Long, Double, Double>>(){});

        DataSet<Tuple3<String, String, Double>> groupDs = latNlong.groupBy(0).reduceGroup(new DistanceReduce());

        //groupDs.print();

        String query = "INSERT INTO flink_jobs.batch_1_8 (trip_id, taxi_id, distance) VALUES (?, ?, ?);";
        groupDs.output(new CassandraTupleOutputFormat<Tuple3<String, String, Double>>(query, clusterBuilder));

        System.out.println("END TIMESTAMP: " + System.currentTimeMillis());

        env1.execute();
    }


}
