package statistics.mapper.separation;

import java.util.Collection;
import java.util.Set;

import scala.Tuple2;
import statistics.formula.FormulaComponentType;

import static java.lang.Math.pow;
import static statistics.formula.FormulaComponentType.COUNT;
import static statistics.formula.FormulaComponentType.DIFF_SQUARED;

/**
 * FormulaSeparator implementation for the Spearman correlation
 */
public class SpearmanFormulaSeparator extends FormulaSeparator {

	@Override
	protected Collection<Tuple2<FormulaComponentType, Double>> getFormulaComponents(Double x, Double y) {
		return Set.of(
				new Tuple2<>(COUNT, Double.valueOf(1)),
				new Tuple2<>(DIFF_SQUARED, pow((x - y), 2))
		);
	}
}
