package statistics;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.ml.linalg.VectorUDT;
import org.apache.spark.ml.stat.Correlation;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import session.SessionWrapper;

public class StatisticsManager {

	public void correlationFromData() {
		List<Row> data = Arrays.asList(
				RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{1.0, -2.0})),
				RowFactory.create(Vectors.dense(4.0, 5.0, 0.0, 3.0)),
				RowFactory.create(Vectors.dense(6.0, 7.0, 0.0, 8.0)),
				RowFactory.create(Vectors.sparse(4, new int[]{0, 3}, new double[]{9.0, 1.0}))
		);

		StructType schema = new StructType(new StructField[]{
				new StructField("features", new VectorUDT(), false, Metadata.empty()),
		});

		SparkSession spark = SessionWrapper.getSession();
		Dataset<Row> df = spark.createDataFrame(data, schema);
		printCorrelationMatrix(df, "features");
	}

	public void correlationFromFile() {
		SparkSession spark = SessionWrapper.getSession();
		Dataset<Row> prices = spark.read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load("src/main/resources/nyse/prices.csv")
				.cache();
		printCorrelationMatrix(prices, "open");
	}

	private void printCorrelationMatrix(Dataset<Row> dataFrame, String features) {
		Row pearson = Correlation.corr(dataFrame, features).head();
		Row spearman = Correlation.corr(dataFrame, features, "spearman").head();

		System.out.println("Pearson correlation matrix:\n" + pearson.get(0).toString());
		System.out.println("Spearman correlation matrix:\n" + spearman.get(0).toString());
	}
}
