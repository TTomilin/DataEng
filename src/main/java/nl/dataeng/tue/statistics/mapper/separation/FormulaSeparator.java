package nl.dataeng.tue.statistics.mapper.separation;

import java.util.Collection;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import nl.dataeng.tue.schema.CorrelationMeasurePair;
import nl.dataeng.tue.schema.entry.DataEntryCollection;
import nl.dataeng.tue.schema.entry.DataEntryPair;
import nl.dataeng.tue.statistics.formula.FormulaComponentKey;
import nl.dataeng.tue.statistics.formula.FormulaComponentType;
import nl.dataeng.tue.statistics.formula.FormulaComponentValue;

/**
 * Abstract separator of a correlation formula
 * Implement the separation of the necessary formula components
 * for a specific correlation to store its components separately
 */
public abstract class FormulaSeparator implements PairFlatMapFunction<DataEntryCollection, FormulaComponentKey, FormulaComponentValue> {

	/**
	 * Maps the given DataEntryPair to formula components required for the statistic calculation
	 * @param collection
	 * @return
	 */
	@Override
	public Iterator<Tuple2<FormulaComponentKey, FormulaComponentValue>> call(DataEntryCollection collection) {
		DataEntryPair pair = (DataEntryPair) collection;
		CorrelationMeasurePair valuePair = pair.getCorrelationMeasurePair();
		double x = valuePair.getFirstValue();
		double y = valuePair.getSecondValue();
		return getFormulaComponents(x, y).stream()
				.map(component -> createTuple(pair, component))
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
	 * Creates a key-value pair from the provided nl.dataeng.tue.data pair and formula components.
	 * The key is a combination of the country pair and the type of the component.
	 * @param pair
	 * @param componentTuple
	 * @return
	 */
	protected Tuple2<FormulaComponentKey, FormulaComponentValue> createTuple(DataEntryPair pair, Tuple2<FormulaComponentType, Double> componentTuple) {
		FormulaComponentType component = componentTuple._1();
		FormulaComponentKey key = new FormulaComponentKey(pair.getCountryPair(), component);
		FormulaComponentValue formulaComponentValue = new FormulaComponentValue(pair.getCountryPair(), component, componentTuple._2());
		return new Tuple2<>(key, formulaComponentValue);
	}
}
