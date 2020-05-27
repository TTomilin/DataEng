package statistics.mapper.computation;

import java.util.Map;

import statistics.formula.FormulaComponentType;
import statistics.formula.FormulaComponentValue;

/**
 * Abstract computer of a correlation statistic
 * Implement the formula calculation for the extending correlation type
 */
public abstract class StatisticComputer extends CorrelationComputer<Map<FormulaComponentType, FormulaComponentValue>> {

}
