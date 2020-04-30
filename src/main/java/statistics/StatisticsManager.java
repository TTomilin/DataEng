package statistics;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.collection.immutable.Set;
import session.SessionWrapper;
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

public class StatisticsManager {

	private static final double THRESHOLD = 0.5;
	private static final String BASE_PATH = "src/main/resources/";
	private static final String WIND_ENERGY = BASE_PATH + "energy-data/wind_generation";
	private static final String SOLAR_ENERGY = BASE_PATH + "energy-data/solar_generation";
	// TODO Remove -  Only for development purposes
	private static final String WIND_ENERGY_REDUCED = BASE_PATH + "energy-data/wind_generation_reduced";

	public void pearsonCorrelation() {
		SparkSession spark = SessionWrapper.getSession();

		JavaPairRDD<FormulaComponent, Double> mappedFormulaComponents = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(WIND_ENERGY_REDUCED)
				.filter((FilterFunction<Row>) row -> !row.anyNull()) // To prevent spark from reading superfluous rows
				.map(
						(MapFunction<Row, ValuePair>) row -> {
							Set<Object> objectSet = row.schema().toSet();
							List<String> names = Arrays.asList(row.schema().names());
							List<String> fieldNames = Arrays.asList(row.schema().fieldNames());
							// Map<String, Object> valuesMap = row.getValuesMap();
//							System.out.println("Length: " + row.length() + " - " + row.toString());
							ValuePair valuePair = new ValuePair(row.getInt(1), row.getInt(5));
							return valuePair;
						},
						Encoders.bean(ValuePair.class))
				.javaRDD()
				.flatMapToPair(new PearsonStatisticMapper())
				.reduceByKey(new FormulaComponentSummator());

		// Execute lazy initialization by running collection to map
		Map<FormulaComponent, Double> formulaComponents = mappedFormulaComponents.collectAsMap();

		double pearson = calculatePearson(formulaComponents);
		System.out.println("Correlation: " + pearson);

		if (pearson > THRESHOLD) {
			// TODO propagate result
		}
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
