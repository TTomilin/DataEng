package statistics.mapper;

import java.util.Collection;
import java.util.Iterator;
import java.util.Set;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import schema.EnergyDataPair;
import statistics.formula.FormulaComponentKey;
import statistics.formula.FormulaComponentType;
import statistics.formula.FormulaComponentValue;

import static java.lang.Math.pow;
import static statistics.formula.FormulaComponentType.COUNT;
import static statistics.formula.FormulaComponentType.DIFF_SQUARED;

/**
 * PairFlatMapFunction implementation for Pearson correlation
 * Maps the given EnergyDataPair to formula components required for the Pearson statistic calculation
 */
public class SpearmanSimpleFormulaSeparator implements PairFlatMapFunction<EnergyDataPair, FormulaComponentKey, FormulaComponentValue> {

	public Iterator<Tuple2<FormulaComponentKey, FormulaComponentValue>> call(EnergyDataPair pair) {
		double x = pair.getEnergyValuePair().getFirstValue();
		double y = pair.getEnergyValuePair().getSecondValue();
		Collection<Tuple2<FormulaComponentKey, FormulaComponentValue>> collection = Set.of(
				createTuple(pair, COUNT, Double.valueOf(1)),
				createTuple(pair, DIFF_SQUARED, pow((x - y), 2))
		);
		return collection.iterator();
	}

	private Tuple2<FormulaComponentKey, FormulaComponentValue> createTuple(EnergyDataPair pair, FormulaComponentType component, double value) {
		FormulaComponentKey key = new FormulaComponentKey(pair.getCountryPair(), component);
		FormulaComponentValue formulaComponentValue = new FormulaComponentValue(pair.getTimestamp(), pair.getCountryPair(), component, value);
		return new Tuple2<>(key, formulaComponentValue);
	}
}
