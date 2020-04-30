package statistics.reducer;

import org.apache.spark.api.java.function.Function2;

public class FormulaComponentSummator implements Function2<Double, Double, Double> {
	/*
	Given any two Floats, this method returns the sum.
	Useful for a reduce operation that adds a stream of numbers.
	 */
	public Double call(Double a, Double b) {
		return a + b;
	}
}
