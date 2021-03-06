package statistics.mapper.computation;

import java.util.Map;

import statistics.formula.FormulaComponentType;
import statistics.formula.FormulaComponentValue;

import static java.lang.Math.pow;
import static statistics.formula.FormulaComponentType.COUNT;
import static statistics.formula.FormulaComponentType.DIFF_SQUARED;

/**
 * Computes the correlation statistic given a map of respectively summed up formula components
 * Utilizes the Spearman correlation formula
 */
public class SpearmanStatisticComputer extends StatisticComputer {

	@Override
	public Double call(Map<FormulaComponentType, FormulaComponentValue> formulaComponents) {
		double count = formulaComponents.get(COUNT).getValue();
		double diffSquared = formulaComponents.get(DIFF_SQUARED).getValue();

		double num = 6 * diffSquared;
		double denom = count * (pow(count, 2) - 1);
		return 1 - num / denom;
	}
}
