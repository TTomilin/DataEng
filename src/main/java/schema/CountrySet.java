package schema;

import java.util.Collection;
import java.util.HashSet;

import scala.Serializable;

public class CountrySet extends HashSet<String> implements CountryCollection, Serializable {

	@Override
	public Collection<String> getCountries() {
		return this;
	}

	@Override
	public String toString() {
		return "[" + String.join(", ", this) + "]";
	}
}
