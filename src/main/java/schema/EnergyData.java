package schema;

import java.sql.Timestamp;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import scala.Serializable;

@Getter
@ToString
@AllArgsConstructor
public class EnergyData implements Serializable {

	private final Timestamp timestamp;
	private final String country;
	private double value;
}
