package schema;

import java.util.Comparator;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import scala.Serializable;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
public class CountryPair implements Serializable, Comparator<CountryPair> {

	private final String firstCountry;
	private final String secondCountry;

	@Override
	public String toString() {
		return String.format("(%s, %s)", firstCountry, secondCountry);
	}

	@Override
	public int compare(CountryPair firstPair, CountryPair secondPair) {
		return Comparator.comparing(CountryPair::getFirstCountry)
				.thenComparing(CountryPair::getSecondCountry)
				.compare(firstPair, secondPair);
	}
}
