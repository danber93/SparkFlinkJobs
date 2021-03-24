import java.io.Serializable;

public class BatchTaxiDist implements Serializable{

	private String taxi_id;
	private Double avg_distance;

	public BatchTaxiDist(String taxi, Double distance) {
		this.taxi_id = taxi;
		this.avg_distance = distance;
	}

	public String getTaxi_id() {
		return taxi_id;
	}

	public void setTaxi_id(String taxi_id) {
		this.taxi_id = taxi_id;
	}

	public Double getAvg_distance() {
		return avg_distance;
	}

	public void setAvg_distance(Double avg_distance) {
		this.avg_distance = avg_distance;
	}

}
