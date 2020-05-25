package schema;

import java.util.HashSet;

import lombok.ToString;
import scala.Serializable;

@ToString
public class CountrySet extends HashSet<String> implements Serializable {

	@Override
	public boolean equals(Object o) {
		return super.equals(o);
	}

	@Override
	public int hashCode() {
		return super.hashCode();
	}
}
