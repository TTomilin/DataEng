package nl.dataeng.tue.schema;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import scala.Serializable;

@Getter
@ToString
@AllArgsConstructor
public class CorrelationMeasurePair implements Serializable {

	private final double firstValue;
	private final double secondValue;
}
