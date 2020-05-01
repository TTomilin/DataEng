package schema;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import scala.Serializable;

public class EnergyPriceEntry implements Serializable {

	private Timestamp timestamp;
	private Map<Locale, EnergyValue> priceMap;

	public EnergyPriceEntry(Timestamp timestamp) {
		this.timestamp = timestamp;
		this.priceMap = new HashMap<>();
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public Map<Locale, EnergyValue> getPriceMap() {
		return priceMap;
	}

	public void setPriceMap(Map<Locale, EnergyValue> priceMap) {
		this.priceMap = priceMap;
	}
}
