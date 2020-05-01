package statistics;

import java.sql.Timestamp;

import scala.Serializable;

public class FormulaValue implements Serializable {

	private final Timestamp timestamp;
	private final CountryPair countryPair;
	private final FormulaComponent component;
	private double value;

	public FormulaValue(Timestamp timestamp, CountryPair countryPair, FormulaComponent component, double value) {
		this.timestamp = timestamp;
		this.countryPair = countryPair;
		this.component = component;
		this.value = value;
	}

	public void increaseValue(double value) {
		this.value += value;
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public CountryPair getCountryPair() {
		return countryPair;
	}

	public FormulaComponent getComponent() {
		return component;
	}

	public double getValue() {
		return value;
	}
}
