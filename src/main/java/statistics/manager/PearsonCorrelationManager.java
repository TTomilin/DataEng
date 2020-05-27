package statistics.manager;

import org.apache.spark.api.java.JavaRDD;

import schema.entry.DataEntry;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.separation.FormulaSeparator;
import statistics.mapper.combinations.PearsonCombinationGenerator;
import statistics.mapper.separation.PearsonFormulaSeparator;
import statistics.mapper.computation.PearsonStatisticComputer;
import statistics.mapper.computation.StatisticComputer;

public class PearsonCorrelationManager extends CorrelationManager {

	private CombinationGenerator generator = new PearsonCombinationGenerator(combinationLength);
	private FormulaSeparator separator = new PearsonFormulaSeparator();
	private StatisticComputer computer = new PearsonStatisticComputer();

	public PearsonCorrelationManager() {
		super(DEFAULT_COMBINATION_LENGTH);
	}

	public PearsonCorrelationManager(Integer combinationLength) {
		super(combinationLength);
	}

	@Override
	protected JavaRDD<DataEntry> applyRanking(JavaRDD<DataEntry> rdd) {
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
