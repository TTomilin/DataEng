package statistics.mapper;

import java.util.Map;

import org.apache.spark.api.java.function.Function;

import statistics.formula.FormulaComponentType;
import statistics.formula.FormulaComponentValue;

import static java.lang.Math.pow;
import static statistics.formula.FormulaComponentType.COUNT;
import static statistics.formula.FormulaComponentType.DIFF_SQUARED;

public class SpearmanSimpleFormulaComputer implements Function<Map<FormulaComponentType, FormulaComponentValue>, Double> {

	@Override
	public Double call(Map<FormulaComponentType, FormulaComponentValue> formulaComponents) {
		double count = formulaComponents.get(COUNT).getValue();
		double diffSquared = formulaComponents.get(DIFF_SQUARED).getValue();

		return 1 - 6 * diffSquared / count * (pow(count, 2) - 1);
	}
}
