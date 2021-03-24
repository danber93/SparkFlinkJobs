package flinkJobs;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class CountReducer implements GroupReduceFunction<Tuple2<String, Double>, Tuple2<String, Integer>> {
    @Override
    public void reduce(Iterable<Tuple2<String, Double>> iterable, Collector<Tuple2<String, Integer>> collector) throws Exception {

        String taxi_id = null;
        int count = 0;

        for(Tuple2<String, Double> t : iterable) {
            if (taxi_id == null)
                taxi_id = t.f0;
            count += 1;

        }

        collector.collect(new Tuple2<String, Integer>(taxi_id, count));
    }
}
