package statistics.formula;

import java.sql.Timestamp;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import scala.Serializable;
import schema.CountryPair;

@Getter
@ToString
@AllArgsConstructor
public class FormulaValue implements Serializable {

	private final Timestamp timestamp;
	private final CountryPair countryPair;
	private final FormulaComponent component;
	private double value;

	public FormulaValue increase(double value) {
		this.value += value;
		return this;
	}
}
