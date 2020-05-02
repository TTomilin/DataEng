package statistics.mapper;

import java.util.Map;

import org.apache.spark.api.java.function.Function;

import statistics.formula.FormulaComponent;
import statistics.formula.FormulaValue;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static statistics.formula.FormulaComponent.COUNT;
import static statistics.formula.FormulaComponent.FIRST_ELEMENT;
import static statistics.formula.FormulaComponent.FIRST_SQUARED;
import static statistics.formula.FormulaComponent.PRODUCT;
import static statistics.formula.FormulaComponent.SECOND_ELEMENT;
import static statistics.formula.FormulaComponent.SECOND_SQUARED;

/**
 * Computes the correlation statistic given a map of respectively summed up formula components
 * Utilizes the pearson correlation formula
 */
public class PearsonStatisticComputer implements Function<Map<FormulaComponent, FormulaValue>, Double> {

	@Override
	public Double call(Map<FormulaComponent, FormulaValue> formulaComponents) {
		double count = formulaComponents.get(COUNT).getValue();
		double sumX = formulaComponents.get(FIRST_ELEMENT).getValue();
		double sumY = formulaComponents.get(SECOND_ELEMENT).getValue();
		double sumXSquared = formulaComponents.get(FIRST_SQUARED).getValue();
		double sumYSquared = formulaComponents.get(SECOND_SQUARED).getValue();
		double XDotY = formulaComponents.get(PRODUCT).getValue();

		double numerator = count * XDotY - sumX * sumY;
		double denominator = sqrt(count * sumXSquared - pow(sumX, 2)) * sqrt(count * sumYSquared - pow(sumY, 2));
		return numerator / denominator;
	}
}
