package statistics.mapper;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import schema.EnergyDataPair;
import statistics.formula.FormulaComponentType;
import statistics.formula.FormulaComponentKey;
import statistics.formula.FormulaComponentValue;

import static java.lang.Math.pow;
import static statistics.formula.FormulaComponentType.COUNT;
import static statistics.formula.FormulaComponentType.FIRST_ELEMENT;
import static statistics.formula.FormulaComponentType.FIRST_SQUARED;
import static statistics.formula.FormulaComponentType.PRODUCT;
import static statistics.formula.FormulaComponentType.SECOND_ELEMENT;
import static statistics.formula.FormulaComponentType.SECOND_SQUARED;

/**
 * PairFlatMapFunction implementation for Pearson correlation
 * Maps the given EnergyDataPair to formula components required for the Pearson statistic calculation
 */
public class PearsonFormulaSeparator implements PairFlatMapFunction<EnergyDataPair, FormulaComponentKey, FormulaComponentValue> {

	public Iterator<Tuple2<FormulaComponentKey, FormulaComponentValue>> call(EnergyDataPair pair) {
		double x = pair.getEnergyValuePair().getFirstValue();
		double y = pair.getEnergyValuePair().getSecondValue();
		Collection<Tuple2<FormulaComponentKey, FormulaComponentValue>> collection = Set.of(
				createTuple(pair, COUNT, Double.valueOf(1)),
				createTuple(pair, FIRST_ELEMENT, x),
				createTuple(pair, SECOND_ELEMENT, y),
				createTuple(pair, FIRST_SQUARED, pow(x, 2)),
				createTuple(pair, SECOND_SQUARED, pow(y, 2)),
				createTuple(pair, PRODUCT, x * y)
		);
		return collection.iterator();
	}

	private Tuple2<FormulaComponentKey, FormulaComponentValue> createTuple(EnergyDataPair pair, FormulaComponentType component, double value) {
		FormulaComponentKey key = new FormulaComponentKey(pair.getCountryPair(), component);
		FormulaComponentValue formulaComponentValue = new FormulaComponentValue(pair.getTimestamp(), pair.getCountryPair(), component, value);
		return new Tuple2<>(key, formulaComponentValue);
	}
}
