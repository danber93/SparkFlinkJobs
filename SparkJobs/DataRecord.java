import java.io.Serializable;
import java.sql.Timestamp;

public class DataRecord implements Serializable {

    private String trip_id;
    private String call_id;
    private String taxi_id;
    private long timestamp_trip_id;
    private String day_id;
    private String missing_data;
    private double lon;
    private double lat;

    public DataRecord(String trip_id, String call_id, String taxi_id, String timestamp_trip_id, String day_id, String missing_data, String lon, String lat) {
        this.trip_id = trip_id;
        this.call_id = call_id;
        this.taxi_id = taxi_id;
        generaTime(timestamp_trip_id);
        this.day_id = day_id;
        this.missing_data = missing_data;
        generaLon(lon);
        generaLat(lat);
        
    }
    
    private void generaTime(String timestamp_trip_id2) {
    	this.timestamp_trip_id = Long.parseLong(timestamp_trip_id2);
	}

	public DataRecord() {}

    public String getTrip_id() {
		return trip_id;
	}

	public void setTrip_id(String trip_id) {
		this.trip_id = trip_id;
	}

	public String getCall_id() {
		return call_id;
	}

	public void setCall_id(String call_id) {
		this.call_id = call_id;
	}

	public String getTaxi_id() {
		return taxi_id;
	}

	public void setTaxi_id(String taxi_id) {
		this.taxi_id = taxi_id;
	}

	public long getTimestamp_trip_id() {
		return timestamp_trip_id;
	}

	public void setTimestamp_trip_id(long timestamp_trip_id) {
		this.timestamp_trip_id = timestamp_trip_id;
	}

	public String getDay_id() {
		return day_id;
	}

	public void setDay_id(String day_id) {
		this.day_id = day_id;
	}

	public String getMissing_data() {
		return missing_data;
	}

	public void setMissing_data(String missing_data) {
		this.missing_data = missing_data;
	}

	public double getLon() {
		return lon;
	}

	public void setLon(double lon) {
		this.lon = lon;
	}

	public double getLat() {
		return lat;
	}

	public void setLat(double lat) {
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		System.out.println(timestamp.getTime());
		this.lat = lat;
	}

	private void generaLat(String lat2) {
    	this.lat = Double.parseDouble(lat2);
	}

	private void generaLon(String lon2) {
		this.lon = Double.parseDouble(lon2);
	}

}
