package schema;

import java.util.Collection;

import org.apache.commons.collections4.CollectionUtils;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import scala.Serializable;

@Getter
@ToString
//@EqualsAndHashCode
@AllArgsConstructor
public class DataEntryCollection implements Serializable {

	private final CountrySet countries;
	private final Collection<Integer> values;

	@Override
	public boolean equals(Object object) {
		if (object == this){
			return true;
		} else if (!(object instanceof DataEntryCollection)) {
			return false;
		}
		DataEntryCollection other = (DataEntryCollection) object;
//		boolean equal = countries.containsAll(other.getCountries()) && values.containsAll(other.getValues());
		boolean equal = CollectionUtils.isEqualCollection(countries, other.getCountries()) && CollectionUtils.isEqualCollection(values, other.getValues());
		if (equal) {
			System.out.println(this + " and " + other + " are equal: " + equal);
		}
		return equal;
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}
}
