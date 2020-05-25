package schema;

import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.commons.collections4.CollectionUtils.isEqualCollection;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class DataEntrySet extends HashSet<DataEntry> {

	private Integer[] marginalOccurrences;

	@Override
	public boolean equals(Object object) {
		if (object == this){
			return true;
		} else if (!(object instanceof DataEntrySet)) {
			return false;
		}
		DataEntrySet other = (DataEntrySet) object;
		return isEqualCollection(getCountries(), other.getCountries()) && isEqualCollection(getValues(), other.getValues());
	}

	@Override
	public String toString() {
		return String.format("%s - %s", getCountries(), getValues());
	}

	@Override
	public int hashCode() {
		return stream().map(DataEntry::getCountry).mapToInt(Objects::hashCode).sum() +
				stream().map(DataEntry::getValue).mapToInt(Objects::hashCode).sum();
	}

	public Set<String> getCountries() {
		return stream().map(DataEntry::getCountry).collect(Collectors.toSet());
	}

	public List<Integer> getValues() {
		return stream().map(DataEntry::getValue).map(Double::intValue).collect(Collectors.toList());
	}
}
