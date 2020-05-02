package statistics;

import java.util.Comparator;

import lombok.AllArgsConstructor;
import lombok.Getter;
import scala.Serializable;

@Getter
@AllArgsConstructor
public class CountryPair implements Serializable, Comparator<CountryPair> {

	private final String firstCountry;
	private final String secondCountry;

	@Override
	public boolean equals(Object obj) {
		if (obj == this) {
			return true;
		} else if (!(obj instanceof CountryPair)) {
			return false;
		}
		CountryPair other = (CountryPair) obj;
		return other.firstCountry.equals(this.firstCountry) && other.secondCountry.equals(this.secondCountry);
	}

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
