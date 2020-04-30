package statistics.mapper;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;
import statistics.ValuePair;

public class PearsonStatisticMapper implements PairFlatMapFunction<ValuePair, Integer, Double> {

	public Iterator<Tuple2<Integer, Double>> call(ValuePair pair) {
		Tuple2<Integer,Double>[] tuples = new Tuple2[6];
		double x = pair.getX();
		double y = pair.getY();
		if(x != 0.0 && y != 0.0) {
			tuples[0] = new Tuple2(0, Double.valueOf(1));
			tuples[1] = new Tuple2(1, x);
			tuples[2] = new Tuple2(2, y);
			tuples[3] = new Tuple2(3, Math.pow(x, 2));
			tuples[4] = new Tuple2(4, Math.pow(y, 2));
			tuples[5] = new Tuple2(5, x * y);
		}
		return Arrays.asList(tuples).iterator();
	}
}
