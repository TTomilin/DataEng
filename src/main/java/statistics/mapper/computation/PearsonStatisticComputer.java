package statistics.mapper.computation;

import java.math.BigDecimal;
import java.util.Map;

import statistics.formula.FormulaComponentType;
import statistics.formula.FormulaComponentValue;

import static java.lang.Math.sqrt;
import static statistics.formula.FormulaComponentType.COUNT;
import static statistics.formula.FormulaComponentType.FIRST_ELEMENT;
import static statistics.formula.FormulaComponentType.FIRST_SQUARED;
import static statistics.formula.FormulaComponentType.PRODUCT;
import static statistics.formula.FormulaComponentType.SECOND_ELEMENT;
import static statistics.formula.FormulaComponentType.SECOND_SQUARED;

/**
 * Computes the correlation statistic given a map of respectively summed up formula components
 * Utilizes the Pearson correlation formula
 */
public class PearsonStatisticComputer extends StatisticComputer {

	@Override
	public Double call(Map<FormulaComponentType, FormulaComponentValue> formulaComponents) {
		BigDecimal count = new BigDecimal(formulaComponents.get(COUNT).getValue());
		BigDecimal sumX = new BigDecimal(formulaComponents.get(FIRST_ELEMENT).getValue());
		BigDecimal sumY = new BigDecimal(formulaComponents.get(SECOND_ELEMENT).getValue());
		BigDecimal sumXSquared = new BigDecimal(formulaComponents.get(FIRST_SQUARED).getValue());
		BigDecimal sumYSquared = new BigDecimal(formulaComponents.get(SECOND_SQUARED).getValue());
		BigDecimal XDotY = new BigDecimal(formulaComponents.get(PRODUCT).getValue());

		BigDecimal numerator = count.multiply(XDotY).subtract(sumX.multiply(sumY));
		double left = sqrt(count.multiply(sumXSquared).subtract(sumX.pow(2)).doubleValue());
		double right = sqrt(count.multiply(sumYSquared).subtract(sumY.pow(2)).doubleValue());
		double denominator = left * right;

		return numerator.doubleValue() / denominator;
	}
}
