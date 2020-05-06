package statistics.mapper;

import java.util.Map;

import org.apache.spark.api.java.function.Function;

import statistics.formula.FormulaComponentType;
import statistics.formula.FormulaComponentValue;

/**
 * Abstract computer of a correlation statistic
 * Implement the formula calculation for the extending correlation type
 */
public abstract class StatisticComputer implements Function<Map<FormulaComponentType, FormulaComponentValue>, Double> {

}
