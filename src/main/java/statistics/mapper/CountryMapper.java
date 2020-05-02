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
 * Remap the tuples by country pair key to further aggregate
 */
public class CountryMapper implements PairFunction<Tuple2<FormulaKey, FormulaValue>, CountryPair, Map<FormulaComponent, FormulaValue>> {

	@Override
	public Tuple2<CountryPair, Map<FormulaComponent, FormulaValue>> call(Tuple2<FormulaKey, FormulaValue> tuple) {
		Map<FormulaComponent, FormulaValue> map = new HashMap<>();
		map.put(tuple._1().getComponent(), tuple._2());
//		Map<FormulaComponent, FormulaValue> map = Map.of(tuple._1().getComponent(), tuple._2());
		return new Tuple2<>(tuple._1().getCountryPair(), map);
	}
}
