package statistics.mapper;

import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import schema.EnergyDataPair;
import schema.EnergyValuePair;
import statistics.formula.FormulaComponentKey;
import statistics.formula.FormulaComponentType;
import statistics.formula.FormulaComponentValue;

/**
 * Implement this abstract separator for a specific correlation
 * type to split up the formula and store its components separately
 */
public abstract class FormulaSeparator implements PairFlatMapFunction<EnergyDataPair, FormulaComponentKey, FormulaComponentValue> {

	/**
	 * Maps the given EnergyDataPair to formula components required for the statistic calculation
	 * @param pair
	 * @return
	 */
	@Override
	public Iterator<Tuple2<FormulaComponentKey, FormulaComponentValue>> call(EnergyDataPair pair) {
		EnergyValuePair valuePair = pair.getEnergyValuePair();
		double x = valuePair.getFirstValue();
		double y = valuePair.getSecondValue();
		return getFormulaComponents(x, y).stream()
				.map(tuple -> createTuple2(pair, tuple))
				.iterator();
	}

	/**
	 * Override to provide the necessary components for the formula of the extending correlation type
	 * @param x
	 * @param y
	 * @return
	 */
	protected abstract Collection<Tuple2<FormulaComponentType, Double>> getFormulaComponents(Double x, Double y);

	/**
	 * Creates a key-value pair from the provided data pair and formula components.
	 * The key is a combination of the country pair and the type of the component.
	 * @param pair
	 * @param componentTuple
	 * @return
	 */
	protected Tuple2<FormulaComponentKey, FormulaComponentValue> createTuple2(EnergyDataPair pair, Tuple2<FormulaComponentType, Double> componentTuple) {
		FormulaComponentType component = componentTuple._1();
		FormulaComponentKey key = new FormulaComponentKey(pair.getCountryPair(), component);
		FormulaComponentValue formulaComponentValue = new FormulaComponentValue(pair.getTimestamp(), pair.getCountryPair(), component, componentTuple._2());
		return new Tuple2<>(key, formulaComponentValue);
	}
}