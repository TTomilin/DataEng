package statistics;

import java.sql.Timestamp;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import scala.Serializable;

@Getter
@ToString
@AllArgsConstructor
public class FormulaValue implements Serializable {

	private final Timestamp timestamp;
	private final CountryPair countryPair;
	private final FormulaComponent component;
	private double value;

	public void increaseValue(double value) {
		this.value += value;
	}
}
