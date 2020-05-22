package schema;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@ToString
@AllArgsConstructor
public class DataEntryPair {

	private final MultiCountryPair countryPair;
	private final CorrelationMeasurePair correlationMeasurePair;
}
