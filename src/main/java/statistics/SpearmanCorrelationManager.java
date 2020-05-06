package statistics;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.api.java.JavaRDD;

import scala.Tuple2;
import schema.DataEntry;
import statistics.mapper.CombinationGenerator;
import statistics.mapper.FormulaSeparator;
import statistics.mapper.SpearmanCombinationGenerator;
import statistics.mapper.SpearmanFormulaSeparator;
import statistics.mapper.SpearmanStatisticComputer;
import statistics.mapper.StatisticComputer;

public class SpearmanCorrelationManager extends CorrelationManager {

	// TODO Determine the best number of partitions
	private static final int NUM_PARTITIONS = 10;

	private CombinationGenerator generator = new SpearmanCombinationGenerator();
	private FormulaSeparator separator = new SpearmanFormulaSeparator();
	private StatisticComputer computer = new SpearmanStatisticComputer();

	@Override
	protected JavaRDD<DataEntry> applyRanking(JavaRDD<DataEntry> javaRDD) {
		return javaRDD
				.sortBy(DataEntry::getValue, Boolean.TRUE, NUM_PARTITIONS)
				.groupBy(DataEntry::getCountry)
				.map(this::rank)
				.flatMap(data -> data._2().iterator());
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

	public Tuple2<String, Iterable<DataEntry>> rank(Tuple2<String, Iterable<DataEntry>> tuples) {
		AtomicInteger count = new AtomicInteger();
		tuples._2().forEach(data -> data.setRank(count.incrementAndGet()));
		return tuples;
	}
}
