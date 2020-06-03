package nl.dataeng.tue.statistics.mapper.separation;

import java.util.Collection;
import java.util.HashSet;

import scala.Tuple2;
import nl.dataeng.tue.statistics.formula.FormulaComponentType;

import static java.lang.Math.pow;
import static nl.dataeng.tue.statistics.formula.FormulaComponentType.COUNT;
import static nl.dataeng.tue.statistics.formula.FormulaComponentType.FIRST_ELEMENT;
import static nl.dataeng.tue.statistics.formula.FormulaComponentType.FIRST_SQUARED;
import static nl.dataeng.tue.statistics.formula.FormulaComponentType.PRODUCT;
import static nl.dataeng.tue.statistics.formula.FormulaComponentType.SECOND_ELEMENT;
import static nl.dataeng.tue.statistics.formula.FormulaComponentType.SECOND_SQUARED;

/**
 * FormulaSeparator implementation for the Pearson correlation
 */
public class PearsonFormulaSeparator extends FormulaSeparator {

	@Override
	protected Collection<Tuple2<FormulaComponentType, Double>> getFormulaComponents(Double x, Double y) {
		Collection<Tuple2<FormulaComponentType, Double>> tuples = new HashSet<>();
		tuples.add(new Tuple2<>(COUNT, Double.valueOf(1)));
		tuples.add(new Tuple2<>(FIRST_ELEMENT, x));
		tuples.add(new Tuple2<>(SECOND_ELEMENT, y));
		tuples.add(new Tuple2<>(FIRST_SQUARED, pow(x, 2)));
		tuples.add(new Tuple2<>(SECOND_SQUARED, pow(y, 2)));
		tuples.add(new Tuple2<>(PRODUCT, x * y));
		return tuples;
	}
}
