package nl.dataeng.tue.statistics.mapper.separation;

import java.util.Collection;
import java.util.HashSet;

import scala.Tuple2;
import nl.dataeng.tue.statistics.formula.FormulaComponentType;

import static java.lang.Math.pow;
import static nl.dataeng.tue.statistics.formula.FormulaComponentType.COUNT;
import static nl.dataeng.tue.statistics.formula.FormulaComponentType.DIFF_SQUARED;

/**
 * FormulaSeparator implementation for the Spearman correlation
 */
public class SpearmanFormulaSeparator extends FormulaSeparator {

	@Override
	protected Collection<Tuple2<FormulaComponentType, Double>> getFormulaComponents(Double x, Double y) {
		Collection<Tuple2<FormulaComponentType, Double>> tuples = new HashSet<>();
		tuples.add(new Tuple2<>(COUNT, Double.valueOf(1)));
		tuples.add(new Tuple2<>(DIFF_SQUARED, pow((x - y), 2)));
		return tuples;
	}
}
