package nl.dataeng.tue.statistics.manager;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import nl.dataeng.tue.schema.country.CountryCollection;
import nl.dataeng.tue.schema.entry.DataEntry;
import nl.dataeng.tue.schema.entry.DataEntryCollection;
import nl.dataeng.tue.statistics.mapper.combinations.CombinationGenerator;
import nl.dataeng.tue.statistics.mapper.combinations.TotalCombinationGenerator;
import nl.dataeng.tue.statistics.mapper.computation.StatisticComputer;
import nl.dataeng.tue.statistics.mapper.computation.TotalCorrelationComputer;
import nl.dataeng.tue.statistics.mapper.separation.FormulaSeparator;
import nl.dataeng.tue.statistics.mapper.wrapper.CountWrapper;
import nl.dataeng.tue.statistics.mapper.wrapper.CountrySetWrapper;

public class TotalCorrelationManager extends CorrelationManager {

	private CombinationGenerator generator = new TotalCombinationGenerator(combinationLength);
	private CountWrapper countWrapper = new CountWrapper();
	private TotalCorrelationComputer computer = new TotalCorrelationComputer();

	public TotalCorrelationManager(Integer combinationLength) {
		super(combinationLength);
	}

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
