package statistics.mapper;

import java.sql.Timestamp;
import java.util.Iterator;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import schema.EnergyValuePair;
import statistics.CountryPair;
import statistics.FormulaComponent;
import statistics.FormulaKey;
import statistics.FormulaValue;

import static java.lang.Math.pow;
import static statistics.FormulaComponent.COUNT;
import static statistics.FormulaComponent.FIRST_ELEMENT;
import static statistics.FormulaComponent.FIRST_SQUARED;
import static statistics.FormulaComponent.PRODUCT;
import static statistics.FormulaComponent.SECOND_ELEMENT;
import static statistics.FormulaComponent.SECOND_SQUARED;

/**
 * PairFlatMapFunction implementation for Pearson correlation
 * Maps the given EnergyValuePair to formula components required for the Pearson statistic calculation
 */
public class PearsonStatisticMapper implements PairFlatMapFunction<EnergyValuePair, FormulaKey, FormulaValue> {

	public Iterator<Tuple2<FormulaKey, FormulaValue>> call(EnergyValuePair pair) {
		double x = pair.getX();
		double y = pair.getY();
		if (x == 0 || y == 0) {
			return null; // This value pair is disregarded if one of the values is missing
		}
		CountryPair countryPair = pair.getCountryPair();
		Timestamp timestamp = pair.getTimestamp();
		Set<Tuple2<FormulaKey, FormulaValue>> tuples = Set.of(
				new Tuple2(new FormulaKey(countryPair, COUNT), new FormulaValue(timestamp, countryPair, COUNT, Double.valueOf(1))),
				new Tuple2(new FormulaKey(countryPair, FIRST_ELEMENT), new FormulaValue(timestamp, countryPair, FIRST_ELEMENT, x)),
				new Tuple2(new FormulaKey(countryPair, SECOND_ELEMENT), new FormulaValue(timestamp, countryPair, SECOND_ELEMENT, y)),
				new Tuple2(new FormulaKey(countryPair, FIRST_SQUARED), new FormulaValue(timestamp, countryPair, FIRST_SQUARED, pow(x, 2))),
				new Tuple2(new FormulaKey(countryPair, SECOND_SQUARED), new FormulaValue(timestamp, countryPair, SECOND_SQUARED, pow(y, 2))),
				new Tuple2(new FormulaKey(countryPair, PRODUCT), new FormulaValue(timestamp, countryPair, PRODUCT, x * y))
		);
		return tuples.iterator();
	}
}
