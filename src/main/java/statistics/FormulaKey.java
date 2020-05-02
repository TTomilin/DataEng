package statistics;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import scala.Serializable;

@Getter
@ToString
@AllArgsConstructor
public class FormulaKey implements Serializable {

	private final CountryPair countryPair;
	private final FormulaComponent component;
}
