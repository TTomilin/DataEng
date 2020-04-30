package schema;

import scala.Serializable;

public class EnergyPrice implements Serializable {

	private double price;

	public EnergyPrice(double price) {
		this.price = price;
	}
}
