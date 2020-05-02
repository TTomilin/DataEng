package statistics;

import java.util.List;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Serializable;
import scala.Tuple2;
import schema.CountryPair;
import schema.EnergyDataPair;
import session.SessionWrapper;
import statistics.mapper.CombinationMapper;
import statistics.mapper.CountryMapper;
import statistics.mapper.PearsonStatisticComputer;
import statistics.mapper.PearsonStatisticMapper;
import statistics.reducer.CountryAggregator;
import statistics.reducer.FormulaComponentSummator;

public class StatisticsManager implements Serializable {

	private static final double THRESHOLD = 0.5;
	private static final String BASE_PATH = "src/main/resources/";
	private static final String WIND_ENERGY = BASE_PATH + "energy-data/wind_generation.csv";
	private static final String SOLAR_ENERGY = BASE_PATH + "energy-data/solar_generation.csv";

	// TODO Remove the following - For development purposes only
	private static final String WIND_ENERGY_REDUCED = BASE_PATH + "energy-data/wind_generation_reduced.csv";
	private static final String WIND_ENERGY_3_COUNTRIES = BASE_PATH + "energy-data/wind_generation_3_countries.csv";

	public void pearsonCorrelation() {
		SparkSession spark = SessionWrapper.getSession();

		List<Tuple2<CountryPair, Double>> collection = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(WIND_ENERGY_3_COUNTRIES)
				.filter((FilterFunction<Row>) row -> !row.anyNull()) // To prevent spark from reading superfluous rows
				.javaRDD()
				.flatMap(new CombinationMapper())
				.filter(this::bothValuesGiven)
				.flatMapToPair(new PearsonStatisticMapper())
				.reduceByKey(new FormulaComponentSummator())
				.mapToPair(new CountryMapper())
				.reduceByKey(new CountryAggregator())
				.mapValues(new PearsonStatisticComputer())
				.collect();
		collection.stream().forEach(this::logCorrelation);
	}

	private boolean bothValuesGiven(EnergyDataPair pair) {
		return pair.getEnergyValuePair().bothValuesPresent();
	}

	private void logCorrelation(Tuple2<CountryPair, Double> tuple) {
		System.out.println("Country Pair " + tuple._1() + ", Correlation: " + tuple._2());
	}
}
