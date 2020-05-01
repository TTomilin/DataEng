package statistics;

import scala.Serializable;

public class FormulaKey implements Serializable {

	private final CountryPair countryPair;
	private final FormulaComponent component;

	public FormulaKey(CountryPair countryPair, FormulaComponent component) {
		this.countryPair = countryPair;
		this.component = component;
	}

	public CountryPair getCountryPair() {
		return countryPair;
	}

	public FormulaComponent getComponent() {
		return component;
	}
}
