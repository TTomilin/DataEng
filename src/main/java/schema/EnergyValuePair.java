package schema;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import scala.Serializable;

@Getter
@ToString
@AllArgsConstructor
public class EnergyValuePair implements Serializable {

	private final double firstValue;
	private final double secondValue;

	public boolean bothValuesPresent() {
		return firstValue != 0 && secondValue != 0;
	}
}
