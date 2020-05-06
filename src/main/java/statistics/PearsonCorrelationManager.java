package statistics;

import org.apache.spark.api.java.JavaRDD;

import schema.DataEntry;
import statistics.mapper.CombinationGenerator;
import statistics.mapper.FormulaSeparator;
import statistics.mapper.PearsonCombinationGenerator;
import statistics.mapper.PearsonFormulaSeparator;
import statistics.mapper.PearsonStatisticComputer;
import statistics.mapper.StatisticComputer;

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
