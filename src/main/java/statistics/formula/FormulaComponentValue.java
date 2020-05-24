package statistics.formula;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import scala.Serializable;
import schema.MultiCountryPair;

@Getter
@ToString
@AllArgsConstructor
public class FormulaComponentValue implements Serializable {

	private final MultiCountryPair countryPair;
	private final FormulaComponentType component;
	private double value;

	public FormulaComponentValue increase(double value) {
		this.value += value;
		return this;
	}
}
