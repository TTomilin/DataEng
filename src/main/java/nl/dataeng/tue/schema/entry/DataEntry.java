package nl.dataeng.tue.schema.entry;

import java.sql.Timestamp;

import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import scala.Serializable;

@Getter
@ToString
@EqualsAndHashCode
@RequiredArgsConstructor
public class DataEntry implements Serializable {

	@NonNull
	private final Timestamp timestamp;

	@NonNull
	@EqualsAndHashCode.Include
	private final String country;

	@EqualsAndHashCode.Include
	private final double value;

	@Setter
	private int rank;
}
