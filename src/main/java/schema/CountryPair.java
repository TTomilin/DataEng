package schema;

import java.util.Comparator;

import lombok.AllArgsConstructor;
import lombok.Getter;
import scala.Serializable;

@Getter
@AllArgsConstructor
@Deprecated
public class CountryPair implements Serializable, Comparator<CountryPair> {

	private final String firstCountry;
	private final String secondCountry;

	@Override
	public String toString() {
		return String.format("Countries (%s, %s)", firstCountry, secondCountry);
	}

	@Override
	public boolean equals(Object object) {
		if (object == this){
			return true;
		} else if (!(object instanceof CountryPair)) {
			return false;
		}
		CountryPair other = (CountryPair) object;
		return other.firstCountry.equals(firstCountry) && other.secondCountry.equals(secondCountry) ||
				other.firstCountry.equals(secondCountry) && other.secondCountry.equals(firstCountry);
	}

	@Override
	public int hashCode() {
		return firstCountry.hashCode() + secondCountry.hashCode();
	}

	@Override
	public int compare(CountryPair firstPair, CountryPair secondPair) {
		return Comparator.comparing(CountryPair::getFirstCountry)
				.thenComparing(CountryPair::getSecondCountry)
				.compare(firstPair, secondPair);
	}
}
