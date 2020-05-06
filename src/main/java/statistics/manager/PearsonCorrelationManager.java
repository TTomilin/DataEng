package statistics.manager;

import org.apache.spark.api.java.JavaRDD;

import schema.DataEntry;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.separation.FormulaSeparator;
import statistics.mapper.combinations.PearsonCombinationGenerator;
import statistics.mapper.separation.PearsonFormulaSeparator;
import statistics.mapper.computation.PearsonStatisticComputer;
import statistics.mapper.computation.StatisticComputer;

public class PearsonCorrelationManager extends CorrelationManager {

	private CombinationGenerator generator = new PearsonCombinationGenerator();
	private FormulaSeparator separator = new PearsonFormulaSeparator();
	private StatisticComputer computer = new PearsonStatisticComputer();

	@Override
	protected JavaRDD<DataEntry> applyRanking(JavaRDD<DataEntry> rdd) {
		// Not needed
		return rdd;
	}

	@Override
	protected CombinationGenerator getCombinationGenerator() {
		return generator;
	}

	@Override
	protected FormulaSeparator getFormulaSeparator() {
		return separator;
	}

	@Override
	protected StatisticComputer getStatisticComputer() {
		return computer;
	}
}
