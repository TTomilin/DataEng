package schema;

import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.collections4.CollectionUtils.isEqualCollection;

import org.apache.commons.collections4.CollectionUtils;

import com.google.common.base.Objects;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
//@ToString
//@EqualsAndHashCode
@AllArgsConstructor
public class DataEntrySet extends HashSet<DataEntry> {

	@Override
	public boolean equals(Object object) {
		if (object == this){
			return true;
		} else if (!(object instanceof DataEntrySet)) {
			return false;
		}
		DataEntrySet other = (DataEntrySet) object;
		boolean equal = isEqualCollection(getCountries(), other.getCountries()) && isEqualCollection(getValues(), other.getValues());
		if (equal) {
			System.out.println(this + " and " + other + " are equal: " + equal);
		}
		return equal;
	}

	@Override
	public String toString() {
		return String.format("%s - %s", getCountries(), getValues());
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}

	public Set<String> getCountries() {
		return stream().map(DataEntry::getCountry).collect(Collectors.toSet());
	}

	public Set<Integer> getValues() {
		return stream().map(DataEntry::getValue).map(Double::intValue).collect(Collectors.toSet());
	}
}
