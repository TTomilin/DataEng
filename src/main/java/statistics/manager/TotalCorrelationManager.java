package statistics.manager;

import java.util.Collection;
import java.util.Map;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;

import data.DataFile;
import scala.Tuple2;
import schema.DataEntry;
import schema.MultiCountryPair;
import statistics.mapper.TotalCountryPairWrapper;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.combinations.TotalCombinationGenerator;
import statistics.mapper.computation.StatisticComputer;
import statistics.mapper.computation.TotalStatisticComputer;
import statistics.mapper.separation.FormulaSeparator;
import statistics.mapper.separation.TotalFormulaSeparator;
import statistics.reducer.BinAggregator;

public class TotalCorrelationManager extends CorrelationManager {

	private static final Integer COMBINATION_LENGTH = 3;

	private TotalCombinationGenerator generator = new TotalCombinationGenerator(COMBINATION_LENGTH);
	private TotalFormulaSeparator separator = new TotalFormulaSeparator();
	private TotalStatisticComputer computer = new TotalStatisticComputer();

	@Override
	protected JavaRDD<DataEntry> applyRanking(JavaRDD<DataEntry> javaRDD) {
		return javaRDD;
	}

	@Override
	protected CombinationGenerator getCombinationGenerator() {
		return null;
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
	public Collection<Tuple2<MultiCountryPair, Double>> calculateCorrelations(DataFile dataFile) {
		JavaRDD<DataEntry> javaRDD = getDataEntryJavaRDD(dataFile);
		applyRanking(javaRDD)
				.groupBy(DataEntry::getTimestamp)
				.flatMap(generator)
				.mapToPair(separator)
				.reduceByKey((v1, v2) -> v1 + v2)
				.mapToPair(new TotalCountryPairWrapper())
				.reduceByKey(new BinAggregator())
				.mapValues(computer)
//				.filter(this::applyThreshold)
				.collect();
		return null;
	}
}
