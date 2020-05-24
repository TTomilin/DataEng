package statistics.mapper.separation;

import java.util.Collection;

import scala.Tuple2;
import statistics.formula.FormulaComponentType;

/**
 * FormulaSeparator implementation for the Total correlation
 */
public class TotalFormulaSeparator extends FormulaSeparator {

	@Override
	protected Collection<Tuple2<FormulaComponentType, Double>> getFormulaComponents(Double x, Double y) {
		return null;
	}
}
