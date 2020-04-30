package statistics.mapper;

import java.util.Iterator;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import statistics.FormulaComponent;
import statistics.ValuePair;

import static java.lang.Math.pow;
import static statistics.FormulaComponent.COUNT;
import static statistics.FormulaComponent.FIRST_ELEMENT;
import static statistics.FormulaComponent.FIRST_SQUARED;
import static statistics.FormulaComponent.PRODUCT;
import static statistics.FormulaComponent.SECOND_ELEMENT;
import static statistics.FormulaComponent.SECOND_SQUARED;

/**
 * PairFlatMapFunction implementation for Pearson correlation
 * Maps the given ValuePair to formula components required for the Pearson statistic calculation
 */
public class PearsonStatisticMapper implements PairFlatMapFunction<ValuePair, FormulaComponent, Double> {

	public Iterator<Tuple2<FormulaComponent, Double>> call(ValuePair pair) {
		double x = pair.getX();
		double y = pair.getY();
		if (x == 0 || y == 0) {
			return null; // This value pair is disregarded if one of the values is missing
		}
		Set<Tuple2<FormulaComponent, Double>> tuples = Set.of(
				new Tuple2(COUNT, Double.valueOf(1)),
				new Tuple2(FIRST_ELEMENT, x),
				new Tuple2(SECOND_ELEMENT, y),
				new Tuple2(FIRST_SQUARED, pow(x, 2)),
				new Tuple2(SECOND_SQUARED, pow(y, 2)),
				new Tuple2(PRODUCT, x * y)
		);
		return tuples.iterator();
	}
}
