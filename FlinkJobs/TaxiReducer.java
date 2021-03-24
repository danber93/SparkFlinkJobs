package flinkJobs;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class TaxiReducer implements GroupReduceFunction<Tuple2<String, Double>, Tuple2<String, Double>> {
    @Override
    public void reduce(Iterable<Tuple2<String, Double>> iterable, Collector<Tuple2<String, Double>> collector) throws Exception {

        String taxi_id = null;
        double sumDis = 0;

        for(Tuple2<String, Double> t : iterable) {
            if (taxi_id == null)
                taxi_id = t.f0;
            sumDis += t.f1;

        }

        collector.collect(new Tuple2<String, Double>(taxi_id, sumDis));
    }
}
