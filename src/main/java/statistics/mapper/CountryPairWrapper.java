package statistics.mapper;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import schema.CountryPair;
import statistics.formula.FormulaComponent;
import statistics.formula.FormulaKey;
import statistics.formula.FormulaValue;

/**
 * In order to calculate the statistic, the formula components
 * need to be grouped together for each country pair beforehand.
 */
public class CountryPairWrapper implements PairFunction<Tuple2<FormulaKey, FormulaValue>, CountryPair, Map<FormulaComponent, FormulaValue>> {

	/**
	 * Remaps the tuples by the country pair key for further aggregation
	 * @param tuple
	 * @return A tuple in which the key is the country pair and the value is
	 * a single element map containing the formula value by the component
	 */
	@Override
	public Tuple2<CountryPair, Map<FormulaComponent, FormulaValue>> call(Tuple2<FormulaKey, FormulaValue> tuple) {
		Map<FormulaComponent, FormulaValue> map = new HashMap<>();
		map.put(tuple._1().getComponent(), tuple._2());
		return new Tuple2<>(tuple._1().getCountryPair(), map);
	}
}
