package statistics.reducer;

import org.apache.spark.api.java.function.Function2;

import statistics.formula.FormulaComponentValue;

/**
 * Reducer implementation to sum two given float values.
 * Used for pairwise summation of formula components.
 */
public class FormulaComponentSummator implements Function2<FormulaComponentValue, FormulaComponentValue, FormulaComponentValue> {

	@Override
	public FormulaComponentValue call(FormulaComponentValue firstValue, FormulaComponentValue secondValue) {
		return firstValue.increase(secondValue.getValue());
	}
}
