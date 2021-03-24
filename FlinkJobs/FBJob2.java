package flinkJobs;

import com.datastax.driver.core.Cluster;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.JoinOperator;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.api.java.tuple.Tuple8;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.batch.connectors.cassandra.CassandraInputFormat;
import org.apache.flink.batch.connectors.cassandra.CassandraTupleOutputFormat;
import org.apache.flink.streaming.connectors.cassandra.ClusterBuilder;

public class FBJob2 {

    public static void main(String[] args) throws Exception {

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
                        (new CassandraInputFormat<Tuple8<String, Long, String, String, Double, Double, String, String>>("select trip_id, timestamp_trip_id, call_id, day_id, lat, longitude, missing_data, taxi_id from flink_jobs.stream_1_8",clusterBuilder)
                                , TupleTypeInfo.of(new TypeHint<Tuple8<String, Long, String, String, Double, Double, String, String>>() {})).filter(value -> value.f6.equals("False"));

        //trip_id, taxi_id, timestamp, lat, long
        DataSet<Tuple5<String, String, Long, Double, Double>> latNlong = inputRecords.map(value -> {
            return new Tuple5<String, String, Long, Double, Double>(value.f0, value.f7, value.f1, value.f4, value.f5);
        }).returns(new TypeHint<Tuple5<String, String, Long, Double, Double>>(){});

        DataSet<Tuple3<String, String, Double>> distances = latNlong.groupBy(0).reduceGroup(new DistanceReduce());

        //
        DataSet<Tuple2<String, Double>> reduceByTaxiId = distances.map(value -> {
             return new Tuple2<>(value.f1, value.f2);
        }).returns(new TypeHint<Tuple2<String, Double>>(){}).groupBy(0).reduceGroup(new TaxiReducer());

        //reduceByTaxiId.print();
        //System.out.println(reduceByTaxiId.count());

        DataSet<Tuple2<String, Integer>> countNumberOfDistancesForEachTaxi = distances.map(value -> {
            return new Tuple2<>(value.f1, value.f2);
        }).returns(new TypeHint<Tuple2<String, Double>>(){}).groupBy(0).reduceGroup(new CountReducer());

        //countNumberOfDistancesForEachTaxi.print();
        //System.out.println(countNumberOfDistancesForEachTaxi.count());

        JoinOperator.DefaultJoin<Tuple2<String, Double>, Tuple2<String, Integer>> joinedDss = reduceByTaxiId.join(countNumberOfDistancesForEachTaxi)
                .where(0).equalTo(0);

        DataSet<Tuple2<String, Double>> avDistance = joinedDss.map(value -> {
            return new Tuple2<String, Double>(value.f0.f0, value.f0.f1 / value.f1.f1);
        }).returns(new TypeHint<Tuple2<String, Double>>(){});

        //avDistance.print();

        String query = "INSERT INTO flink_jobs.batch_2_8 (taxi_id, avDistance) VALUES (?, ?);";
        avDistance.output(new CassandraTupleOutputFormat<Tuple2<String, Double>>(query, clusterBuilder));

        env1.execute();
    }
}
