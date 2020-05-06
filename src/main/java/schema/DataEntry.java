package schema;

import java.sql.Timestamp;

import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import scala.Serializable;

@Getter
@ToString
@RequiredArgsConstructor
public class DataEntry implements Serializable {

	@NonNull private final Timestamp timestamp;
	@NonNull private final String country;
	private final double value;
	@Setter private int rank;
}
