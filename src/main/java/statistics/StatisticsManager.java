package statistics;

import java.util.Collection;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

import data.EnergyType;
import scala.Serializable;
import scala.Tuple2;
import schema.CountryPair;
import schema.EnergyDataPair;
import session.SessionWrapper;
import statistics.mapper.CombinationGenerator;
import statistics.mapper.CountryPairWrapper;
import statistics.mapper.PearsonStatisticComputer;
import statistics.mapper.PearsonFormulaSeparator;
import statistics.reducer.FormulaComponentAggregator;
import statistics.reducer.FormulaComponentSummator;

public class StatisticsManager implements Serializable {

	private static final double THRESHOLD = 0.5;
	private static final String PATH_TEMPLATE = "src/main/resources/energy-data/%s.csv";

	public Collection<Tuple2<CountryPair, Double>> pearsonCorrelations(EnergyType energyType) {
		return SessionWrapper.getSession().read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(String.format(PATH_TEMPLATE, energyType.getFileName()))
				.filter((FilterFunction<Row>) row -> !row.anyNull()) // To prevent spark from reading superfluous rows
				.javaRDD()
				.flatMap(new CombinationGenerator())
				.filter(this::bothValuesGiven)
				.flatMapToPair(new PearsonFormulaSeparator())
				.reduceByKey(new FormulaComponentSummator())
				.mapToPair(new CountryPairWrapper())
				.reduceByKey(new FormulaComponentAggregator())
				.mapValues(new PearsonStatisticComputer())
//				.filter(this::applyThreshold) // Temporarily removed to display all values
				.collect();
	}

	private boolean bothValuesGiven(EnergyDataPair pair) {
		return pair.getEnergyValuePair().bothValuesPresent();
	}

	private boolean applyThreshold(Tuple2<CountryPair, Double> tuple) {
		return tuple._2() >= THRESHOLD;
	}
}
