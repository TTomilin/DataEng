package statistics;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.paukov.combinatorics.CombinatoricsFactory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import scala.Tuple2;
import scala.collection.Iterable;
import schema.EnergyValue;
import schema.EnergyValuePair;
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
	private static final String WIND_ENERGY = BASE_PATH + "energy-data/wind_generation.csv";
	private static final String SOLAR_ENERGY = BASE_PATH + "energy-data/solar_generation.csv";
	// TODO Remove -  Only for development purposes
	private static final String WIND_ENERGY_REDUCED = BASE_PATH + "energy-data/wind_generation_reduced.csv";

	public void pearsonCorrelation() {
		SparkSession spark = SessionWrapper.getSession();

		JavaPairRDD<FormulaKey, FormulaValue> mappedFormulaComponents = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(WIND_ENERGY_REDUCED)
				.filter((FilterFunction<Row>) row -> !row.anyNull()) // To prevent spark from reading superfluous rows
				.flatMap(
						(FlatMapFunction<Row, EnergyValuePair>) row -> {

							List<EnergyValue> energyValueList = new ArrayList<>();
							Timestamp timestamp = row.getTimestamp(0);
							Iterable<StructField> columns = row.schema().toSeq().drop(1); // Drop timestamp
							columns.foreach(struct -> {
								String countryCode = struct.name();
								boolean isStringInstance = struct.dataType().typeName().equalsIgnoreCase(Integer.class.getSimpleName());
								Double value = isStringInstance ?
										(double) ((Integer) row.getAs(countryCode)).intValue() :
										(double) row.getAs(countryCode);
								System.out.println("Country " + countryCode + ", value: " + value);
								EnergyValue energyValue = new EnergyValue(timestamp, countryCode, value);
								energyValueList.add(energyValue);
								return energyValue;
							});

							ICombinatoricsVector<EnergyValue> initialVector = CombinatoricsFactory.createVector(energyValueList);
							Generator<EnergyValue> generator = CombinatoricsFactory.createSimpleCombinationGenerator(initialVector, 2);
							List<ICombinatoricsVector<EnergyValue>> iCombinatoricsVectors = generator.generateAllObjects();

							Iterator<ICombinatoricsVector<EnergyValue>> iterator = iCombinatoricsVectors.iterator();
							List<EnergyValuePair> valuePairs = new ArrayList<>();

							// TODO replace with stream
							while (iterator.hasNext()) {
								ICombinatoricsVector<EnergyValue> energyValues = iterator.next();
								List<EnergyValue> vector = energyValues.getVector();
								EnergyValue firstValue = vector.get(0);
								EnergyValue secondValue = vector.get(1);
								valuePairs.add(new EnergyValuePair(firstValue.getTimestamp(),
										new CountryPair(firstValue.getCountry(), secondValue.getCountry()),
										firstValue.getValue(), firstValue.getValue())
								);
							}

							/*List<EnergyValuePair> valuePairs = iCombinatoricsVectors.stream()
									.map(this::toValuePair)
									.collect(Collectors.toList());*/
							return valuePairs.iterator();
//							return new EnergyValuePair(timestamp, "EE", "NL", row.getInt(1), row.getInt(5));
						},
						Encoders.bean(EnergyValuePair.class)
				)
				.javaRDD()
				.flatMapToPair(new PearsonStatisticMapper())
				.reduceByKey(new FormulaComponentSummator());

		// Execute lazy initialization by running collection to map
		List<Tuple2<FormulaKey, FormulaValue>> formulaComponents = mappedFormulaComponents.collect();
		Map<CountryPair, List<Tuple2<FormulaKey, FormulaValue>>> collect = formulaComponents.stream().collect(Collectors.groupingBy(entry -> entry._1().getCountryPair()));
		Map<FormulaComponent, FormulaValue> formulaValueMap = collect.values().stream()
				.flatMap(List::stream)
				.collect(Collectors.toMap(tuple -> tuple._1().getComponent(), tuple -> tuple._2()));

		//		double pearson = calculatePearson(formulaComponents);
		double pearson = 5;
		System.out.println("Correlation: " + pearson);

		if (pearson > THRESHOLD) {
			// TODO propagate result
		}
	}

	private EnergyValuePair toValuePair(ICombinatoricsVector<EnergyValue> vector) {
		EnergyValue firstValue = vector.getValue(0);
		EnergyValue secondValue = vector.getValue(1);
		return new EnergyValuePair(firstValue.getTimestamp(),
				new CountryPair(firstValue.getCountry(), secondValue.getCountry()),
				firstValue.getValue(), firstValue.getValue());
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
