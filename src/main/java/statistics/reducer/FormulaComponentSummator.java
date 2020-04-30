package statistics.reducer;

import org.apache.spark.api.java.function.Function2;

/**
 * Reducer implementation to sum two given float values.
 * Used for pairwise summation of formula components.
 */
public class FormulaComponentSummator implements Function2<Double, Double, Double> {

	public Double call(Double a, Double b) {
		return a + b;
	}
}
