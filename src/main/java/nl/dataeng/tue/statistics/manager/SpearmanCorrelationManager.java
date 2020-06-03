package nl.dataeng.tue.statistics.manager;

import org.apache.spark.api.java.JavaRDD;

import nl.dataeng.tue.schema.entry.DataEntry;
import nl.dataeng.tue.statistics.mapper.SpearmanIncrementalRanker;
import nl.dataeng.tue.statistics.mapper.combinations.CombinationGenerator;
import nl.dataeng.tue.statistics.mapper.combinations.SpearmanCombinationGenerator;
import nl.dataeng.tue.statistics.mapper.computation.SpearmanStatisticComputer;
import nl.dataeng.tue.statistics.mapper.computation.StatisticComputer;
import nl.dataeng.tue.statistics.mapper.separation.FormulaSeparator;
import nl.dataeng.tue.statistics.mapper.separation.SpearmanFormulaSeparator;

public class SpearmanCorrelationManager extends CorrelationManager {

	private static final int NUM_PARTITIONS = 10;

	private CombinationGenerator generator = new SpearmanCombinationGenerator(combinationLength);
	private FormulaSeparator separator = new SpearmanFormulaSeparator();
	private StatisticComputer computer = new SpearmanStatisticComputer();
	private SpearmanIncrementalRanker ranker = new SpearmanIncrementalRanker();

	public SpearmanCorrelationManager() {
		super(DEFAULT_COMBINATION_LENGTH);
	}

	@Override
	protected JavaRDD<DataEntry> applyRanking(JavaRDD<DataEntry> javaRDD) {
		return javaRDD
				.sortBy(DataEntry::getValue, Boolean.TRUE, NUM_PARTITIONS)
				.groupBy(DataEntry::getCountry)
				.flatMap(ranker);
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
