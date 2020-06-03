package nl.dataeng.tue.statistics.mapper.computation;

import java.util.Map;

import nl.dataeng.tue.statistics.formula.FormulaComponentType;
import nl.dataeng.tue.statistics.formula.FormulaComponentValue;

/**
 * Abstract computer of a correlation statistic
 * Implement the formula calculation for the extending correlation type
 */
public abstract class StatisticComputer extends CorrelationComputer<Map<FormulaComponentType, FormulaComponentValue>> {

}
