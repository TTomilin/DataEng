package schema.entry;

import java.util.Arrays;
import java.util.Collection;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import schema.CorrelationMeasurePair;
import schema.country.MultiCountryPair;

@Getter
@ToString
@AllArgsConstructor
public class DataEntryPair implements DataEntryCollection {

	private final MultiCountryPair countryPair;
	private final CorrelationMeasurePair correlationMeasurePair;

	@Override
	public Collection<String> getCountries() {
		return countryPair.getCountries();
	}

	@Override
	public Collection<Double> getValues() {
		return Arrays.asList(correlationMeasurePair.getFirstValue(), correlationMeasurePair.getSecondValue());
	}
}
