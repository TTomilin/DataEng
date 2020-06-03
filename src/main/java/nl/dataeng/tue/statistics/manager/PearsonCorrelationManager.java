package nl.dataeng.tue.statistics.manager;

import org.apache.spark.api.java.JavaRDD;

import nl.dataeng.tue.schema.entry.DataEntry;
import nl.dataeng.tue.statistics.mapper.combinations.CombinationGenerator;
import nl.dataeng.tue.statistics.mapper.separation.FormulaSeparator;
import nl.dataeng.tue.statistics.mapper.combinations.PearsonCombinationGenerator;
import nl.dataeng.tue.statistics.mapper.separation.PearsonFormulaSeparator;
import nl.dataeng.tue.statistics.mapper.computation.PearsonStatisticComputer;
import nl.dataeng.tue.statistics.mapper.computation.StatisticComputer;

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
