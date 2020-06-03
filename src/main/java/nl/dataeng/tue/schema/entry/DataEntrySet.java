package nl.dataeng.tue.schema.entry;

import java.util.Collection;
import java.util.HashSet;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.commons.collections4.CollectionUtils.isEqualCollection;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class DataEntrySet extends HashSet<DataEntry> implements DataEntryCollection {

	private Integer count;

	@Override
	public Collection<String> getCountries() {
		return stream().map(DataEntry::getCountry).collect(Collectors.toSet());
	}

	@Override
	public Collection<Double> getValues() {
		return stream().map(DataEntry::getValue).collect(Collectors.toList());
	}

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
}
