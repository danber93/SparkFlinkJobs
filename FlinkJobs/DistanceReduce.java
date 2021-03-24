package flinkJobs;

import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.util.Collector;

import java.util.*;

public class DistanceReduce  implements GroupReduceFunction<Tuple5<String, String, Long, Double, Double>, Tuple3<String, String, Double>> {


    @Override
    public void reduce(Iterable<Tuple5<String, String, Long, Double, Double>> iterable, Collector<Tuple3<String, String, Double>> collector) throws Exception {

        List<Long> timestamps = new ArrayList<>();

        Map<Long, Tuple2<Double, Double>> latNlong = new HashMap<>();

        String trip_id = null;
        String taxi_id = null;

        for(Tuple5<String, String, Long, Double, Double> in : iterable) {
            if(trip_id == null)
                trip_id = in.f0;
            if(taxi_id == null)
                taxi_id = in.f1;

            timestamps.add(in.f2);
            latNlong.put(in.f2, new Tuple2<>(in.f3, in.f4));
        }

        timestamps.sort(null);

        double currDistance = 0;
        for (int i=0; i<=timestamps.size()-2; i++) {

            currDistance += distance(latNlong.get(timestamps.get(i)).f0, latNlong.get(timestamps.get(i)).f1,
                    latNlong.get(timestamps.get(i+1)).f0, latNlong.get(timestamps.get(i+1)).f1);

        }

        collector.collect(new Tuple3<>(trip_id, taxi_id, currDistance));
    }

    public double distance(double lat1, double longit1, double lat2, double longit2) {

        double distance = 0;

        lat1 = lat1*Math.PI / 180;
        longit1 = longit1*Math.PI / 180;

        lat2 = lat2*Math.PI / 180;
        longit2 = longit2*Math.PI / 180;

        double dist_long = longit2 - longit1;
        double dist_lat = lat2 - lat1;

        double pezzo1 = Math.cos(lat2)*Math.sin(dist_long);
        double pezzo11 = pezzo1*pezzo1;

        double pezzo2 = Math.cos(lat1)*Math.sin(lat2)-Math.sin(lat1)*Math.cos(lat2)*Math.cos(dist_long);
        double pezzo22 = pezzo2*pezzo2;

        double pezzo3 = Math.sin(lat1)*Math.sin(lat2)+Math.cos(lat1)*Math.cos(lat2)*Math.cos(dist_long);

        double pezzo4 = Math.atan((Math.sqrt(pezzo11+pezzo22))/pezzo3);

        distance = pezzo4*6372;

        distance = Math.abs(distance);

        return distance;
    }
}
