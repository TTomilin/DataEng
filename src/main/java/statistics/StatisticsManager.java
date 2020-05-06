package statistics;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

import data.DataFile;
import scala.Serializable;
import scala.Tuple2;
import schema.CountryPair;
import schema.EnergyData;
import schema.EnergyDataPair;
import schema.RankedEnergyData;
import session.SessionWrapper;
import statistics.mapper.CombinationGenerator;
import statistics.mapper.CountryPairWrapper;
import statistics.mapper.NewCombinationGenerator;
import statistics.mapper.PearsonFormulaSeparator;
import statistics.mapper.PearsonStatisticComputer;
import statistics.mapper.SpearmanSimpleFormulaSeparator;
import statistics.reducer.FormulaComponentAggregator;
import statistics.reducer.FormulaComponentSummator;

public class StatisticsManager implements IManager, Serializable {

	private static final double THRESHOLD = 0.5;
	private static final String PATH_TEMPLATE = "src/main/resources/energy-data/%s.csv";
	public static final int NUM_PARTITIONS = 10;
	private CombinationGenerator combinationGenerator = new CombinationGenerator();
	private PearsonFormulaSeparator formulaSeparator = new PearsonFormulaSeparator();
	private FormulaComponentSummator componentSummator = new FormulaComponentSummator();
	private CountryPairWrapper countryPairWrapper = new CountryPairWrapper();
	private FormulaComponentAggregator componentAggregator = new FormulaComponentAggregator();
	private PearsonStatisticComputer statisticComputer = new PearsonStatisticComputer();

	@Override
	public Collection<Tuple2<CountryPair, Double>> correlations(CorrelationType correlationType, DataFile dataFile) {
		return null;
	}

	public Collection<Tuple2<CountryPair, Double>> spearmanCorrelations(DataFile dataFile) {
		getJavaRDDStream(dataFile)
				.flatMap(this::toEnergyValues)
				.filter(this::valueProvided)
				.sortBy(EnergyData::getValue, true, NUM_PARTITIONS)
				.groupBy(EnergyData::getCountry)
				.map(this::rank)
				.flatMap(data -> data._2().iterator())
				.groupBy(EnergyData::getTimestamp)
				.flatMap(new NewCombinationGenerator())
				.flatMapToPair(new SpearmanSimpleFormulaSeparator())
				.reduceByKey(componentSummator)
				.collect();
		return null;
	}

	public Collection<Tuple2<CountryPair, Double>> pearsonCorrelations(DataFile dataFile) {
		return getJavaRDDStream(dataFile)
				.flatMap(combinationGenerator)
				.filter(this::bothValuesProvided)
				.flatMapToPair(formulaSeparator)
				.reduceByKey(componentSummator)
				.mapToPair(countryPairWrapper)
				.reduceByKey(componentAggregator)
				.mapValues(statisticComputer)
				.filter(this::applyThreshold) // Temporarily removed to display all values
				.collect();
	}

	private JavaRDD<Row> getJavaRDDStream(DataFile dataFile) {
		return SessionWrapper.getSession().read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(String.format(PATH_TEMPLATE, dataFile.getFileName()))
				.filter((FilterFunction<Row>) row -> !row.anyNull()) // To prevent spark from reading superfluous rows
				.javaRDD();
	}

	private boolean valueProvided(EnergyData data) {
		return data.getValue() != 0;
	}
	private boolean bothValuesProvided(EnergyDataPair pair) {
		return pair.getEnergyValuePair().bothValuesPresent();
	}

	private Iterator<RankedEnergyData> toEnergyValues(Row row) {
		List<RankedEnergyData> energyEntries = new ArrayList<>();
		Timestamp timestamp = row.getTimestamp(0);
		row.schema().toList().drop(1).foreach(field -> { // Drop timestamp and iterate over fields
			String countryCode = field.name();
			Double value = row.getAs(countryCode);
			RankedEnergyData energyData = new RankedEnergyData(timestamp, countryCode, value);
			energyEntries.add(energyData);
			return energyData;
		});
		return energyEntries.iterator();
	}

	public Tuple2<String, Iterable<RankedEnergyData>> rank(Tuple2<String, Iterable<RankedEnergyData>> tuples) {
		AtomicInteger count = new AtomicInteger();
		tuples._2().forEach(data -> data.setRank(count.incrementAndGet()));
		return tuples;
	}

	private boolean applyThreshold(Tuple2<CountryPair, Double> tuple) {
		return tuple._2() >= THRESHOLD;
	}
}
