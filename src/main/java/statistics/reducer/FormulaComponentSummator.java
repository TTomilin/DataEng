package statistics.reducer;

import org.apache.spark.api.java.function.Function2;

import statistics.formula.FormulaValue;

/**
 * Reducer implementation to sum two given float values.
 * Used for pairwise summation of formula components.
 */
public class FormulaComponentSummator implements Function2<FormulaValue, FormulaValue, FormulaValue> {

	public FormulaValue call(FormulaValue firstValue, FormulaValue secondValue) {
		return new FormulaValue(firstValue.getTimestamp(), firstValue.getCountryPair(),
				firstValue.getComponent(), firstValue.getValue() + secondValue.getValue());
	}
}
