package statistics.manager;

import java.util.Collection;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

import data.DataFile;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import scala.Serializable;
import scala.Tuple2;
import schema.country.CountryCollection;
import schema.entry.DataEntry;
import schema.entry.DataEntryCollection;
import session.SessionWrapper;
import statistics.mapper.EnergyDataConverter;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.computation.StatisticComputer;
import statistics.mapper.separation.FormulaSeparator;
import statistics.mapper.wrapper.CountryPairWrapper;
import statistics.reducer.FormulaComponentAggregator;
import statistics.reducer.FormulaComponentSummator;

@Getter
@RequiredArgsConstructor
public abstract class CorrelationManager implements ICorrelationManager, Serializable {

	protected static final double THRESHOLD = 0.5;
	protected static final Integer DEFAULT_COMBINATION_LENGTH = 2;
//	protected static final String PATH_TEMPLATE = "src/main/resources/energy-data/%s.csv";
	protected static final String PATH_TEMPLATE = "%s.csv";

	protected FormulaComponentSummator componentSummator = new FormulaComponentSummator();
	protected CountryPairWrapper countryPairWrapper = new CountryPairWrapper();
	protected FormulaComponentAggregator componentAggregator = new FormulaComponentAggregator();
	protected EnergyDataConverter converter = new EnergyDataConverter();

	protected final Integer combinationLength;

	@Override
	public Collection<Tuple2<CountryCollection, Double>> calculateCorrelations(DataFile dataFile) {
		JavaRDD<DataEntry> javaRDD = getDataEntryJavaRDD(dataFile);
		JavaRDD<DataEntryCollection> combinationsJavaRDD = applyRanking(javaRDD)
				.groupBy(DataEntry::getTimestamp)
				.flatMap(getCombinationGenerator());
		return getStatistics(combinationsJavaRDD)
				.filter(this::applyThreshold)
				.collect();
	}

	protected abstract JavaRDD<DataEntry> applyRanking(JavaRDD<DataEntry> javaRDD);
	protected abstract CombinationGenerator getCombinationGenerator();
	protected abstract FormulaSeparator getFormulaSeparator();
	protected abstract StatisticComputer getStatisticComputer();

	protected JavaRDD<DataEntry> getDataEntryJavaRDD(DataFile dataFile) {
		return SessionWrapper.getSession().read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(String.format(PATH_TEMPLATE, dataFile.getFileName()))
				.filter((FilterFunction<Row>) row -> !row.anyNull()) // Prevent Spark from reading superfluous rows
				.javaRDD()
				.flatMap(converter)
				.filter(this::valueProvided);
	}

	protected boolean valueProvided(DataEntry data) {
		return data.getValue() != 0;
	}

	protected JavaPairRDD<CountryCollection, Double> getStatistics(JavaRDD<DataEntryCollection> combinationsJavaRDD) {
		return combinationsJavaRDD
				.flatMapToPair(getFormulaSeparator())
				.reduceByKey(componentSummator)
				.mapToPair(countryPairWrapper)
				.reduceByKey(componentAggregator)
				.mapValues(getStatisticComputer());
	}

	protected boolean applyThreshold(Tuple2<CountryCollection, Double> tuple) {
		return tuple._2() >= THRESHOLD;
	}
}
