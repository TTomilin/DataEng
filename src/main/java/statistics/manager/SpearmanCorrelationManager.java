package statistics.manager;

import org.apache.spark.api.java.JavaRDD;

import schema.entry.DataEntry;
import statistics.mapper.SpearmanIncrementalRanker;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.combinations.SpearmanCombinationGenerator;
import statistics.mapper.computation.SpearmanStatisticComputer;
import statistics.mapper.computation.StatisticComputer;
import statistics.mapper.separation.FormulaSeparator;
import statistics.mapper.separation.SpearmanFormulaSeparator;

public class SpearmanCorrelationManager extends CorrelationManager {

	private static final int NUM_PARTITIONS = 10;

	private CombinationGenerator generator = new SpearmanCombinationGenerator();
	private FormulaSeparator separator = new SpearmanFormulaSeparator();
	private StatisticComputer computer = new SpearmanStatisticComputer();
	private SpearmanIncrementalRanker ranker = new SpearmanIncrementalRanker();

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
