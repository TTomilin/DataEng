package schema;

import java.sql.Timestamp;

import scala.Serializable;

public class EnergyValue implements Serializable {

	private final Timestamp timestamp;
	private final String country;
	private double value;

	public EnergyValue(Timestamp timestamp, String country, double value) {
		this.timestamp = timestamp;
		this.country = country;
		this.value = value;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public String getCountry() {
		return country;
	}

	public double getValue() {
		return value;
	}
}
