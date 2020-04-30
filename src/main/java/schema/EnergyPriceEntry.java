package schema;

import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import scala.Serializable;

public class EnergyPriceEntry implements Serializable {

	private Timestamp timestamp;
	private Map<Locale, EnergyPrice> priceMap;

	public EnergyPriceEntry(Timestamp timestamp) {
		this.timestamp = timestamp;
		this.priceMap = new HashMap<>();
	}

	public void addPrice(Locale locale, double value) {
		EnergyPrice price = new EnergyPrice(value);
		priceMap.put(locale, price);
	}

	public Timestamp getTimestamp() {
		return timestamp;
	}

	public void setTimestamp(Timestamp timestamp) {
		this.timestamp = timestamp;
	}

	public Map<Locale, EnergyPrice> getPriceMap() {
		return priceMap;
	}

	public void setPriceMap(Map<Locale, EnergyPrice> priceMap) {
		this.priceMap = priceMap;
	}
}
