package schema;

import java.sql.Timestamp;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NonNull;
import lombok.ToString;
import scala.Serializable;

@Getter
@ToString
@AllArgsConstructor
public class EnergyData implements Serializable {

	@NonNull private final Timestamp timestamp;
	@NonNull private final String country;
	private final double value;
}
