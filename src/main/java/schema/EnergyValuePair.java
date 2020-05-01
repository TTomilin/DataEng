package schema;

import java.sql.Timestamp;

import statistics.CountryPair;

public class EnergyValuePair {

	private final Timestamp timestamp;
	private final CountryPair countryPair;
	private final double x;
	private final double y;

	public EnergyValuePair(Timestamp timestamp, CountryPair countryPair, double x, double y) {
		this.timestamp = timestamp;
		this.countryPair = countryPair;
		this.x = x;
		this.y = y;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public CountryPair getCountryPair() {
		return countryPair;
	}

	public double getX() {
		return x;
	}

	public double getY() {
		return y;
	}
}
