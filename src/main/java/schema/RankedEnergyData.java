package schema;

import java.sql.Timestamp;

import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;

@Getter
@Setter
public class RankedEnergyData extends EnergyData {

	private int rank;

	public RankedEnergyData(@NonNull Timestamp timestamp, @NonNull String country, double value) {
		super(timestamp, country, value);
	}
}
