package statistics.formula;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import scala.Serializable;
import schema.country.MultiCountryPair;

@Getter
@ToString
@EqualsAndHashCode
@AllArgsConstructor
public class FormulaComponentKey implements Serializable {

	private final MultiCountryPair countryPair;
	private final FormulaComponentType component;
}
