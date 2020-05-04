package statistics.reducer;

import org.apache.spark.api.java.function.Function2;

import statistics.formula.FormulaValue;

/**
 * Reducer implementation to sum two given float values.
 * Used for pairwise summation of formula components.
 */
public class FormulaComponentSummator implements Function2<FormulaValue, FormulaValue, FormulaValue> {

	@Override
	public FormulaValue call(FormulaValue firstValue, FormulaValue secondValue) {
		return firstValue.increase(secondValue.getValue());
	}
}
