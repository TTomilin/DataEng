package statistics;

import java.util.Collection;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

import data.DataFile;
import scala.Serializable;
import scala.Tuple2;
import schema.CountryPair;
import schema.EnergyDataPair;
import session.SessionWrapper;
import statistics.mapper.CountryPairWrapper;
import statistics.mapper.FormulaSeparator;
import statistics.mapper.StatisticComputer;
import statistics.reducer.FormulaComponentAggregator;
import statistics.reducer.FormulaComponentSummator;

public abstract class CorrelationManager implements ICorrelationManager, Serializable {

	private static final double THRESHOLD = 0.5;
	private static final String PATH_TEMPLATE = "src/main/resources/energy-data/%s.csv";
	private FormulaComponentSummator componentSummator = new FormulaComponentSummator();
	private CountryPairWrapper countryPairWrapper = new CountryPairWrapper();
	private FormulaComponentAggregator componentAggregator = new FormulaComponentAggregator();

	@Override
	public Collection<Tuple2<CountryPair, Double>> calculateCorrelations(DataFile dataFile) {
		JavaRDD<Row> rdd = getRowJavaRDD(dataFile);
		JavaRDD<EnergyDataPair> dataPairs = preprocess(rdd);
		return dataPairs
				.flatMapToPair(getFormulaSeparator())
				.reduceByKey(componentSummator)
				.mapToPair(countryPairWrapper)
				.reduceByKey(componentAggregator)
				.mapValues(getStatisticComputer())
				.filter(this::applyThreshold)
				.collect();
	}

	protected abstract JavaRDD<EnergyDataPair> preprocess(JavaRDD<Row> rdd);
	protected abstract FormulaSeparator getFormulaSeparator();
	protected abstract StatisticComputer getStatisticComputer();

	private JavaRDD<Row> getRowJavaRDD(DataFile dataFile) {
		return SessionWrapper.getSession().read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(String.format(PATH_TEMPLATE, dataFile.getFileName()))
				.filter((FilterFunction<Row>) row -> !row.anyNull()) // Prevent Spark from reading superfluous rows
				.javaRDD();
	}

	private boolean applyThreshold(Tuple2<CountryPair, Double> tuple) {
		return tuple._2() >= THRESHOLD;
	}
}
