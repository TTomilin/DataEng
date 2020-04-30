package statistics.mapper;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import statistics.FormulaComponent;
import statistics.ValuePair;

import static statistics.FormulaComponent.*;

public class PearsonStatisticMapper implements PairFlatMapFunction<ValuePair, FormulaComponent, Double> {

	public static final Integer FORMULA_COMPONENTS = 6;

	public Iterator<Tuple2<FormulaComponent, Double>> call(ValuePair pair) {
		Tuple2<FormulaComponent, Double>[] tuples = new Tuple2[FORMULA_COMPONENTS];
		double x = pair.getX();
		double y = pair.getY();
		if(x != 0.0 && y != 0.0) {
			tuples[0] = new Tuple2(COUNT, Double.valueOf(1));
			tuples[1] = new Tuple2(FIRST_ELEMENT, x);
			tuples[2] = new Tuple2(SECOND_ELEMENT, y);
			tuples[3] = new Tuple2(FIRST_SQUARED, Math.pow(x, 2));
			tuples[4] = new Tuple2(SECOND_SQUARED, Math.pow(y, 2));
			tuples[5] = new Tuple2(PRODUCT, x * y);
		}
		return Arrays.asList(tuples).iterator();
	}
}
