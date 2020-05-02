package statistics;

import java.util.Map;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Serializable;
import scala.Tuple2;
import schema.EnergyDataPair;
import session.SessionWrapper;
import statistics.mapper.CombinationMapper;
import statistics.mapper.CountryMapper;
import statistics.mapper.PearsonStatisticMapper;
import statistics.reducer.FormulaComponentSummator;

import static java.lang.Math.pow;
import static java.lang.Math.sqrt;
import static statistics.FormulaComponent.COUNT;
import static statistics.FormulaComponent.FIRST_ELEMENT;
import static statistics.FormulaComponent.FIRST_SQUARED;
import static statistics.FormulaComponent.PRODUCT;
import static statistics.FormulaComponent.SECOND_ELEMENT;
import static statistics.FormulaComponent.SECOND_SQUARED;

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

		Map<CountryPair, java.lang.Iterable<Tuple2<FormulaComponent, FormulaValue>>> countryPairIterableMap = spark.read()
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
				.groupByKey()
				.collectAsMap();
	}

	private boolean bothValuesGiven(EnergyDataPair pair) {
		return pair.getEnergyValuePair().bothValuesPresent();
	}

	/**
	 * Utilizes the standard correlation formula
	 * @param formulaComponents Map of respectively summed up values to compute the statistic
	 * @return Pearson statistic
	 */
	private double calculatePearson(Map<FormulaComponent, Double> formulaComponents) {
		double count = formulaComponents.get(COUNT);
		double sumX = formulaComponents.get(FIRST_ELEMENT);
		double sumY = formulaComponents.get(SECOND_ELEMENT);
		double sumXSquared = formulaComponents.get(FIRST_SQUARED);
		double sumYSquared = formulaComponents.get(SECOND_SQUARED);
		double XDotY = formulaComponents.get(PRODUCT);

		double numerator = count * XDotY - sumX * sumY;
		double denominator = sqrt(count * sumXSquared - pow(sumX, 2)) * sqrt(count * sumYSquared - pow(sumY, 2));
		return numerator / denominator;
	}
}
