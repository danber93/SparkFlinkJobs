import java.io.Serializable;

public class BatchDataRecord implements Serializable {

    private String trip;
    private String call;
    private String taxi;
    private String timestamp;
    private String day;
    private String missing;
	private double lon;
	private double lat;

    public BatchDataRecord(String trip, String call, String taxi, String timestamp, String day, String missing, String polyline) {
        this.trip = trip;
        this.call = call;
        this.taxi = taxi;
        this.timestamp = timestamp;
        this.day = day;
        this.missing = missing;
        setLatLon(polyline);
    }

	public BatchDataRecord(String trip, String call, String taxi, String timestamp, String day, String missing, double lon, double lat) {
		this.trip = trip;
        this.call = call;
        this.taxi = taxi;
        this.timestamp = timestamp;
        this.day = day;
        this.missing = missing;
        this.lat = lat;
        this.lon = lon;
	}

	public String getTrip() {
        return trip;
    }

    public void setTrip(String trip) {
        this.trip = trip;
    }

    public String getCall() {
        return call;
    }

    public void setCall(String call) {
        this.call = call;
    }

    public String getTaxi() {
        return taxi;
    }

    public void setTaxi(String taxi) {
        this.taxi = taxi;
    }

    public String getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(String timestamp) {
        this.timestamp = timestamp;
    }

    public String getDay() {
        return day;
    }

    public void setDay(String day) {
        this.day = day;
    }

    public String getMissing() {
        return missing;
    }

    public void setMissing(String missing) {
        this.missing = missing;
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
		this.lat = lat;
	}

    public void setLatLon(String polyline) {
    	polyline = polyline.replace("[", "").replace("]", "").replace("\"", " ");
    	String[] s = polyline.split(",");
    	this.lon = Double.parseDouble(s[0]);
    	this.lat = Double.parseDouble(s[1]);
    }

}
