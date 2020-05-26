package statistics.manager;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import schema.country.CountryCollection;
import schema.entry.DataEntry;
import schema.entry.DataEntryCollection;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.combinations.TotalCombinationGenerator;
import statistics.mapper.computation.StatisticComputer;
import statistics.mapper.computation.TotalCorrelationComputer;
import statistics.mapper.separation.FormulaSeparator;
import statistics.mapper.wrapper.CountWrapper;
import statistics.mapper.wrapper.CountrySetWrapper;

public class TotalCorrelationManager extends CorrelationManager {

	private static final Integer COMBINATION_LENGTH = 3;

	private CombinationGenerator generator = new TotalCombinationGenerator(COMBINATION_LENGTH);
	private CountWrapper countWrapper = new CountWrapper();
	private TotalCorrelationComputer computer = new TotalCorrelationComputer();

	@Override
	protected JavaRDD<DataEntry> applyRanking(JavaRDD<DataEntry> javaRDD) {
		return javaRDD;
	}

	@Override
	protected CombinationGenerator getCombinationGenerator() {
		return generator;
	}

	@Override
	protected FormulaSeparator getFormulaSeparator() {
		return null;
	}

	@Override
	protected StatisticComputer getStatisticComputer() {
		return null;
	}

	@Override
	protected JavaPairRDD<CountryCollection, Double> getStatistics(JavaRDD<DataEntryCollection> combinationsJavaRDD) {
		return combinationsJavaRDD
				.mapToPair(countWrapper)
				.reduceByKey((firstCount, secondCount) -> firstCount + secondCount)
				.mapToPair(new CountrySetWrapper())
				.groupByKey()
				.mapValues(computer);
	}
}
