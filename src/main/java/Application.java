import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;

import org.apache.log4j.lf5.LogLevel;

import data.DataFile;
import scala.Tuple2;
import schema.CountryCollection;
import schema.MultiCountryPair;
import session.SessionWrapper;
import statistics.Aggregator;
import statistics.CorrelationType;
import statistics.manager.CorrelationManager;

import static data.DataFile.WIND_100ROWS;
import static data.DataFile.WIND_10ROWS_3COUNTRIES_DISC;
import static data.DataFile.WIND_10ROWS_6COUNTRIES_DISC;
import static data.DataFile.WIND_10ROWS_7COUNTRIES;
import static statistics.Aggregator.AVG;
import static statistics.CorrelationType.PEARSON_MULTI;
import static statistics.CorrelationType.TOTAL;

public class Application {

	private static LogLevel logLevel = LogLevel.WARN;

	public static void main(String[] args) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		setHadoopHome(args);
		SessionWrapper.setLogLevel(logLevel);

		// Milestone 1
//		correlation(PEARSON, WIND);
//		correlation(SPEARMAN, WIND);
//		correlation(PEARSON, SOLAR);
//		correlation(SPEARMAN, SOLAR);

		// Milestone 2
//		correlation(PEARSON_MULTI, WIND_10ROWS_6COUNTRIES_DISC, AVG);
//		correlation(PEARSON_MULTI, WIND_100ROWS, AVG);

		correlation(TOTAL, WIND_10ROWS_6COUNTRIES_DISC);
	}

	private static void setHadoopHome(String[] args) {
		// Add the location of hadoop binaries as a program argument if the JVM cannot find the HADOOP_HOME
		if (!Arrays.asList(args).isEmpty()) {
			System.setProperty("hadoop.home.dir", args[0]);
		}
	}

	private static void correlation(CorrelationType type, DataFile file, Aggregator... aggregator) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		logCorrelationStart(type, file);
		CorrelationManager manager = aggregator.length > 0 ?
				createManagerWithAggregator(type, aggregator[0]) :
				type.getManager().getDeclaredConstructor().newInstance();
		manager.calculateCorrelations(file).forEach(Application::logCorrelation);
	}

	private static CorrelationManager createManagerWithAggregator(CorrelationType type, Aggregator aggregator) throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
		return type.getManager().getDeclaredConstructor(Aggregator.class).newInstance(aggregator);
	}

	private static void logCorrelationStart(CorrelationType type, DataFile file) {
		System.out.println();
		System.out.println(String.format("Calculating %s correlation of %s data", type, file));
	}

	private static void logCorrelation(Tuple2<CountryCollection, Double> tuple) {
		System.out.println(String.format("%s, Correlation: %.5f", tuple._1(), tuple._2()));
	}
}
