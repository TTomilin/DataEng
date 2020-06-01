package statistics.mapper.wrapper;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import schema.country.CountryCollection;
import statistics.formula.FormulaComponentKey;
import statistics.formula.FormulaComponentType;
import statistics.formula.FormulaComponentValue;

/**
 * In order to calculate the statistic, the formula components
 * need to be grouped together for each country pair beforehand.
 */
public class CountryPairWrapper implements PairFunction<Tuple2<FormulaComponentKey, FormulaComponentValue>, CountryCollection, Map<FormulaComponentType, FormulaComponentValue>> {

	/**
	 * Remaps the tuples by the country pair key for further aggregation
	 * @param tuple
	 * @return A tuple in which the key is the country pair and the value is
	 * a single element map containing the formula value by the component
	 */
	@Override
	public Tuple2<CountryCollection, Map<FormulaComponentType, FormulaComponentValue>> call(Tuple2<FormulaComponentKey, FormulaComponentValue> tuple) {
		Map<FormulaComponentType, FormulaComponentValue> map = new HashMap<>();
		map.put(tuple._1().getComponent(), tuple._2());
		return new Tuple2<>(tuple._1().getCountryPair(), map);
	}
}
