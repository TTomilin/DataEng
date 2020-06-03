package nl.dataeng.tue.schema.country;

import java.util.Arrays;
import java.util.Collection;

import org.apache.commons.collections4.CollectionUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import scala.Serializable;

@Getter
@AllArgsConstructor
public class MultiCountryPair implements CountryCollection, Serializable {

	Collection<String> firstCollection;
	Collection<String> secondCollection;

	@Override
	public Collection<String> getCountries() {
		return CollectionUtils.union(firstCollection, secondCollection);
	}

	@Override
	public String toString() {
		return String.format("Countries %s and %s",
				Arrays.toString(firstCollection.toArray()),
				Arrays.toString(secondCollection.toArray()));
	}

	@Override
	public boolean equals(Object object) {
		if (object == this){
			return true;
		} else if (!(object instanceof MultiCountryPair)) {
			return false;
		}
		MultiCountryPair other = (MultiCountryPair) object;
		return
				CollectionUtils.isEqualCollection(firstCollection, other.getFirstCollection()) &&
				CollectionUtils.isEqualCollection(secondCollection, other.getSecondCollection()) ||
				CollectionUtils.isEqualCollection(firstCollection, other.getSecondCollection()) &&
				CollectionUtils.isEqualCollection(secondCollection, other.getFirstCollection());
	}

	@Override
	public int hashCode() {
		return CollectionUtils.union(firstCollection, secondCollection).stream()
				.mapToInt(Object::hashCode)
				.sum();
	}
}
