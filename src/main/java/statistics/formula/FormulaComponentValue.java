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
public class FormulaComponentValue implements Serializable {

	private final Timestamp timestamp;
	private final CountryPair countryPair;
	private final FormulaComponentType component;
	private double value;

	public FormulaComponentValue increase(double value) {
		this.value += value;
		return this;
	}
}
