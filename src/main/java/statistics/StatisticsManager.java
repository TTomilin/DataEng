package statistics;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import scala.Tuple2;
import scala.collection.immutable.Map;
import scala.collection.immutable.Set;
import schema.EnergyPriceEntry;
import session.SessionWrapper;
import statistics.mapper.PearsonStatisticMapper;
import statistics.reducer.FormulaComponentSummator;

public class StatisticsManager {

	private static final double THRESHOLD = 0.5;
	private static final String BASE_PATH = "src/main/resources/";

	public void pearsonCorrelation() {
		String path = BASE_PATH + "energy-data/wind_generation_reduced.csv";
		SparkSession spark = SessionWrapper.getSession();

		JavaPairRDD<FormulaComponent, Double> mappedFormulaComponents = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(path)
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

		// Execute lazy initialization
		List<Tuple2<FormulaComponent, Double>> output = mappedFormulaComponents.collect();

		double count = 0, sumX = 0, sumY = 0, sumXSq = 0, sumYSq = 0, xDotY = 0;
		for (Tuple2<FormulaComponent, Double> tuple : output) {
			switch (tuple._1()) {
			case COUNT: count = tuple._2();
				break;
			case FIRST_ELEMENT: sumX = tuple._2();
				break;
			case SECOND_ELEMENT: sumY = tuple._2();
				break;
			case FIRST_SQUARED: sumXSq = tuple._2();
				break;
			case SECOND_SQUARED: sumYSq = tuple._2();
				break;
			case PRODUCT: xDotY = tuple._2();
				break;
			default:
				throw new RuntimeException("Unknown formula component: " + tuple._1().name());
			}
		}

		double numerator = count * xDotY - sumX * sumY;
		double denominator = Math.sqrt(count * sumXSq - Math.pow(sumX, 2)) * Math.sqrt(count * sumYSq - Math.pow(sumY, 2));
		double pearson = numerator / denominator;
		System.out.println("Correlation:" + pearson);
	}

	private static boolean applyThreshold(double correlation) {
		return correlation > THRESHOLD;
	}

	private EnergyPriceEntry mapToEnergyPrice(Row row) {
		return new EnergyPriceEntry(row.getTimestamp(1));
	}

	private double correlationFunction(double[] prices1, double[] prices2) {
		PearsonsCorrelation correlation = new PearsonsCorrelation();
		return correlation.correlation(prices1, prices2);
	}
}
