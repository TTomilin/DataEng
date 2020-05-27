package statistics.manager;

import java.util.Collection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

import data.DataFile;
import scala.Serializable;
import scala.Tuple2;
import schema.DataEntry;
import schema.MultiCountryPair;
import session.SessionWrapper;
import statistics.mapper.CountryPairWrapper;
import statistics.mapper.EnergyDataConverter;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.computation.StatisticComputer;
import statistics.mapper.separation.FormulaSeparator;
import statistics.reducer.FormulaComponentAggregator;
import statistics.reducer.FormulaComponentSummator;

public abstract class CorrelationManager implements ICorrelationManager, Serializable {

	private static final double THRESHOLD = 0;
	private static final String PATH_TEMPLATE = "src/main/resources/energy-data/%s.csv";
	private FormulaComponentSummator componentSummator = new FormulaComponentSummator();
	private CountryPairWrapper countryPairWrapper = new CountryPairWrapper();
	private FormulaComponentAggregator componentAggregator = new FormulaComponentAggregator();
	private EnergyDataConverter converter = new EnergyDataConverter();

	@Override
	public Collection<Tuple2<MultiCountryPair, Double>> calculateCorrelations(DataFile dataFile) {
		JavaRDD<DataEntry> javaRDD = getDataEntryJavaRDD(dataFile);
		return applyRanking(javaRDD)
				.groupBy(DataEntry::getTimestamp)
				.flatMap(getCombinationGenerator())
				.flatMapToPair(getFormulaSeparator())
				.reduceByKey(componentSummator)
				.mapToPair(countryPairWrapper)
				.reduceByKey(componentAggregator)
				.mapValues(getStatisticComputer())
				.filter(this::applyThreshold)
				.collect();
	}

	protected abstract JavaRDD<DataEntry> applyRanking(JavaRDD<DataEntry> javaRDD);
	protected abstract CombinationGenerator getCombinationGenerator();
	protected abstract FormulaSeparator getFormulaSeparator();
	protected abstract StatisticComputer getStatisticComputer();

	private boolean valueProvided(DataEntry data) {
		return data.getValue() != 0;
	}

	private JavaRDD<DataEntry> getDataEntryJavaRDD(DataFile dataFile) {
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

	private boolean applyThreshold(Tuple2<MultiCountryPair, Double> tuple) {
		return tuple._2() >= THRESHOLD;
	}
}
