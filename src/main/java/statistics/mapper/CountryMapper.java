package statistics.mapper;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import statistics.CountryPair;
import statistics.FormulaComponent;
import statistics.FormulaKey;
import statistics.FormulaValue;

/**
 * Remap the tuples by country pair key to further aggregate
 */
public class CountryMapper implements PairFunction<Tuple2<FormulaKey, FormulaValue>, CountryPair, Tuple2<FormulaComponent, FormulaValue>> {

	@Override
	public Tuple2<CountryPair, Tuple2<FormulaComponent, FormulaValue>> call(Tuple2<FormulaKey, FormulaValue> tuple) {
		return new Tuple2<>(tuple._1().getCountryPair(), new Tuple2<>(tuple._1().getComponent(), tuple._2()));
	}
}
