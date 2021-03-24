import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import scala.Tuple3;
import scala.Tuple5;

public class BatchTripDist implements Serializable{

	private String trip_id;
	private String taxi_id;
	private double distance;

	public BatchTripDist(String trip_id, String taxi_id, double distance) {
		this.trip_id = trip_id;
		this.taxi_id = taxi_id;
		this.distance = distance;
	}

	public  BatchTripDist(Iterable<Tuple5<String, Long, Double, Double, String>> group) {
		List<Tuple5<String,Long,Double, Double, String>> list = new ArrayList<>();
		//scorro l'iterable e popolo la lista
		group.forEach(row -> list.add(row));
		int length = list.size();
		//se ho almeno un elemento
		this.taxi_id = list.get(0)._5();
		this.trip_id = list.get(0)._1();
		//ordino le tuple
		Collections.sort(list, new MyComparator());
		double dist = 0;
		//calcolo la distanza su tutte le coppie ordinate
		for(int i=0; i<=length-2; i++) {
			dist += distanza(list.get(i)._3(), list.get(i)._4(), list.get(i+1)._3(), list.get(i+1)._4());
		}
		this.distance = dist;
	}
	
	//metodo che calcola la distanza tra due punti geografici date latitudine e longitudine
	private static double distanza(double longitA, double latA, double longitB, double latB) {
		//trasformazione in radianti
		double lat1 = latA*Math.PI / 180;
		double longit1 = longitA*Math.PI / 180;
		double lat2 = latB*Math.PI / 180;
		double longit2 = longitB*Math.PI / 180;

		double distance = 0;
		double dist_long = longit2 - longit1;
		double dist_lat = lat2 - lat1;
		double pezzo1 = Math.cos(lat2)*Math.sin(dist_long);
		double pezzo11 = pezzo1*pezzo1;
		double pezzo2 = Math.cos(lat1)*Math.sin(lat2)-Math.sin(lat1)*Math.cos(lat2)*Math.cos(dist_long);
		double pezzo22 = pezzo2*pezzo2;
		double pezzo3 = Math.sin(lat1)*Math.sin(lat2)+Math.cos(lat1)*Math.cos(lat2)*Math.cos(dist_long);
		double pezzo4 = Math.atan((Math.sqrt(pezzo11+pezzo22))/pezzo3);

		distance = pezzo4*6372;
		
		if(distance>=0)
			return distance;
		else 
			return distance*(-1);
	}

	public String getTrip_id() {
		return trip_id;
	}

	public void setTrip_id(String trip_id) {
		this.trip_id = trip_id;
	}

	public String getTaxi_id() {
		return taxi_id;
	}

	public void setTaxi_id(String taxi_id) {
		this.taxi_id = taxi_id;
	}

	public double getDistance() {
		return distance;
	}

	public void setDistance(double distance) {
		this.distance = distance;
	}

}
