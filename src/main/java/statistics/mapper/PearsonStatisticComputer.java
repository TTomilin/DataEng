package statistics.mapper;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Map;

import org.apache.spark.api.java.function.Function;

import statistics.formula.FormulaComponent;
import statistics.formula.FormulaValue;

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

	private static final int PRECISION = 5;
	private final MathContext mathContext = new MathContext(PRECISION);

	@Override
	public Double call(Map<FormulaComponent, FormulaValue> formulaComponents) {
		BigDecimal count = new BigDecimal(formulaComponents.get(COUNT).getValue());
		BigDecimal sumX = new BigDecimal(formulaComponents.get(FIRST_ELEMENT).getValue());
		BigDecimal sumY = new BigDecimal(formulaComponents.get(SECOND_ELEMENT).getValue());
		BigDecimal sumXSquared = new BigDecimal(formulaComponents.get(FIRST_SQUARED).getValue());
		BigDecimal sumYSquared = new BigDecimal(formulaComponents.get(SECOND_SQUARED).getValue());
		BigDecimal XDotY = new BigDecimal(formulaComponents.get(PRODUCT).getValue());

		BigDecimal numerator = count.multiply(XDotY).subtract(sumX.multiply(sumY));
		BigDecimal left = count.multiply(sumXSquared).subtract(sumX.pow(2)).sqrt(mathContext);
		BigDecimal right = count.multiply(sumYSquared).subtract(sumY.pow(2)).sqrt(mathContext);
		BigDecimal denominator = left.multiply(right);

		return numerator.divide(denominator).doubleValue();
	}
}
