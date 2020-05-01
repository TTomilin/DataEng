package statistics;

import scala.Serializable;

public class CountryPair implements Serializable {
	private final String firstCountry;
	private final String secondCountry;

	public CountryPair(String firstCountry, String secondCountry) {
		this.firstCountry = firstCountry;
		this.secondCountry = secondCountry;
	}

	public String getFirstCountry() {
		return firstCountry;
	}

	public String getSecondCountry() {
		return secondCountry;
	}
}
