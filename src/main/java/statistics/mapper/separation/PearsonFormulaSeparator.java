package statistics.mapper.separation;

import java.util.Collection;
import java.util.Set;

import scala.Tuple2;
import statistics.formula.FormulaComponentType;

import static java.lang.Math.pow;
import static statistics.formula.FormulaComponentType.COUNT;
import static statistics.formula.FormulaComponentType.FIRST_ELEMENT;
import static statistics.formula.FormulaComponentType.FIRST_SQUARED;
import static statistics.formula.FormulaComponentType.PRODUCT;
import static statistics.formula.FormulaComponentType.SECOND_ELEMENT;
import static statistics.formula.FormulaComponentType.SECOND_SQUARED;

/**
 * FormulaSeparator implementation for the Pearson correlation
 */
public class PearsonFormulaSeparator extends FormulaSeparator {

	@Override
	protected Collection<Tuple2<FormulaComponentType, Double>> getFormulaComponents(Double x, Double y) {
		return Set.of(
				new Tuple2<>(COUNT, Double.valueOf(1)),
				new Tuple2<>(FIRST_ELEMENT, x),
				new Tuple2<>(SECOND_ELEMENT, y),
				new Tuple2<>(FIRST_SQUARED, pow(x, 2)),
				new Tuple2<>(SECOND_SQUARED, pow(y, 2)),
				new Tuple2<>(PRODUCT, x * y)
		);
	}
}
