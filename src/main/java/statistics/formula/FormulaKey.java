package statistics.formula;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import scala.Serializable;
import schema.CountryPair;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class FormulaKey implements Serializable {

	private final CountryPair countryPair;
	private final FormulaComponent component;
}
