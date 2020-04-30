package statistics;

import java.util.Arrays;
import java.util.Locale;

import org.apache.commons.math3.stat.correlation.PearsonsCorrelation;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructType;

import scala.collection.Seq;
import schema.EnergyPriceEntry;
import session.SessionWrapper;

public class StatisticsManager {

	public static final double THRESHOLD = 0.5;

	public void correlationFromFile() {
		SparkConf sparkConf = new SparkConf()
				.setAppName("DataEng")
				.setMaster("local[2]")
				.set("spark.executor.memory", "2g");
		JavaSparkContext jc = new JavaSparkContext(sparkConf);

		String path = "src/main/resources/power-sys/reduced_dataset.csv";
		SparkSession spark = SessionWrapper.getSession();
		Dataset<EnergyPriceEntry> rows = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(path)
				.map(
						(MapFunction<Row, EnergyPriceEntry>) row -> {
							StructType schema = row.schema();
							String[] strings = schema.fieldNames();

							Seq<Object> objectSeq = row.toSeq();
							EnergyPriceEntry priceEntry = new EnergyPriceEntry(row.getTimestamp(1));
							Arrays.stream(row.schema().fieldNames()).forEach(name -> {
								String countryCode = name.substring(0, 2);
								Locale locale = new Locale("", countryCode);
								Object value = row.getAs(name);
								if (value.getClass() == Double.class) {
									priceEntry.addPrice(locale, (Double) value);
								}
							});
							return priceEntry;
						},
						Encoders.bean(EnergyPriceEntry.class)
				);
//				.map((MapFunction<EnergyPriceEntry, Double>) price -> this.correlationFunction(
//						price.getPrices1(),
//						price.getPrices2()
//				))
//				.filter(this::applyThreshold)
//				.collect();

		rows.printSchema();
		rows.show(10);
		System.out.println("Dataframe has " + rows.count() + " rows and " + rows.columns().length + " columns.");
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
