import java.util.Arrays;

import data.DataFile;
import scala.Tuple2;
import schema.CountryPair;
import statistics.StatisticsManager;

import static data.DataFile.SOLAR;
import static data.DataFile.WIND;
import static data.DataFile.WIND_100ROWS;
import static data.DataFile.WIND_10ROWS_3COUNTRIES;

public class Application {

	private static StatisticsManager manager;

	public static void main(String[] args) {
		setHadoopHome(args);
		manager = new StatisticsManager();
		spearmanCorrelation(WIND);
//		pearsonCorrelation(WIND_100ROWS);
		pearsonCorrelation(WIND);
//		pearsonCorrelation(SOLAR);
	}

	private static void setHadoopHome(String[] args) {
		// Add the location of hadoop binaries as a program argument if the JVM cannot find the HADOOP_HOME
		if (!Arrays.asList(args).isEmpty()) {
			System.setProperty("hadoop.home.dir", args[0]);
		}
	}

	private static void spearmanCorrelation(DataFile type) {
		manager.spearmanCorrelations(type).forEach(Application::logCorrelation);
	}

	private static void pearsonCorrelation(DataFile type) {
		manager.pearsonCorrelations(type).forEach(Application::logCorrelation);
	}

	private static void logCorrelation(Tuple2<CountryPair, Double> tuple) {
		System.out.println(String.format("%s, Correlation: %.5f", tuple._1(), tuple._2()));
	}
}
