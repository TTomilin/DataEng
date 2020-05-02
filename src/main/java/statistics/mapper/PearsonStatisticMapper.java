package statistics.mapper;

import java.util.Iterator;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import schema.EnergyDataPair;
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
 * Maps the given EnergyDataPair to formula components required for the Pearson statistic calculation
 */
public class PearsonStatisticMapper implements PairFlatMapFunction<EnergyDataPair, FormulaKey, FormulaValue> {

	public Iterator<Tuple2<FormulaKey, FormulaValue>> call(EnergyDataPair pair) {
		double x = pair.getEnergyValuePair().getFirstValue();
		double y = pair.getEnergyValuePair().getSecondValue();
		return Set.of(
				createTuple(pair, COUNT, Double.valueOf(1)),
				createTuple(pair, FIRST_ELEMENT, x),
				createTuple(pair, SECOND_ELEMENT, y),
				createTuple(pair, FIRST_SQUARED, pow(x, 2)),
				createTuple(pair, SECOND_SQUARED, pow(x, y)),
				createTuple(pair, PRODUCT, x * y)
		).iterator();
	}

	private Tuple2<FormulaKey, FormulaValue> createTuple(EnergyDataPair pair, FormulaComponent component, double value) {
		FormulaKey key = new FormulaKey(pair.getCountryPair(), component);
		FormulaValue formulaValue = new FormulaValue(pair.getTimestamp(), pair.getCountryPair(), component, value);
		return new Tuple2<>(key, formulaValue);
	}
}
