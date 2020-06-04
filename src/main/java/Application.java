import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import org.apache.log4j.lf5.LogLevel;

import data.DataFile;
import scala.Tuple2;
import schema.country.CountryCollection;
import session.SessionWrapper;
import statistics.Aggregator;
import statistics.CorrelationType;
import statistics.manager.CorrelationManager;
import statistics.manager.PearsonCorrelationManager;
import statistics.manager.PearsonMultiCorrelationManager;
import statistics.manager.SpearmanCorrelationManager;
import statistics.manager.TotalCorrelationManager;

import static data.DataFile.*;
import static statistics.Aggregator.AVG;
import static statistics.Aggregator.MIN;
import static statistics.Aggregator.MAX;
import static statistics.CorrelationType.PEARSON;
import static statistics.CorrelationType.PEARSON_MULTI;
import static statistics.CorrelationType.SPEARMAN;
import static statistics.CorrelationType.TOTAL;

public class Application {

	private static Map<CorrelationType, CorrelationManager> managers;
	private static LogLevel logLevel = LogLevel.WARN;
	private static final Integer P_VALUE = 3; // Define the p-value here

	public static void main(String[] args) {
		setHadoopHome(args);
		SessionWrapper.setLogLevel(logLevel);
		initializeManagers();

		// Milestone 1
		// correlation(PEARSON, WIND);
		// correlation(SPEARMAN, WIND);
		// correlation(PEARSON, SOLAR);
		// correlation(SPEARMAN, SOLAR);

		// Milestone 2
//		correlation(PEARSON_MULTI, WIND_1000ROWS, Optional.of(AVG));
		correlation(PEARSON_MULTI, SOLAR_1YEAR, Optional.of(MIN));
		correlation(PEARSON_MULTI, SOLAR_2YEAR, Optional.of(MIN));
		correlation(PEARSON_MULTI, SOLAR_3YEAR, Optional.of(MIN));
		correlation(PEARSON_MULTI, SOLAR_4YEAR, Optional.of(MIN));
//		correlation(PEARSON_MULTI, WIND, Optional.of(MIN));
//		correlation(PEARSON_MULTI, WIND, Optional.of(MAX));


//		correlation(TOTAL, WIND_DISCRETIZED);
//		correlation(TOTAL, SOLAR_DISCRETIZED);

		SessionWrapper.getSession().stop();
	}

	private static void setHadoopHome(String[] args) {
		// Add the location of hadoop binaries as a program argument if the JVM cannot find the HADOOP_HOME
		if (!Arrays.asList(args).isEmpty()) {
			System.setProperty("hadoop.home.dir", args[0]);
		}
	}

	private static void initializeManagers() {
		managers = new HashMap<>();
		managers.put(PEARSON, new PearsonCorrelationManager());
		managers.put(SPEARMAN, new SpearmanCorrelationManager());
		managers.put(PEARSON_MULTI, new PearsonMultiCorrelationManager(P_VALUE));
		managers.put(TOTAL, new TotalCorrelationManager(P_VALUE));
	}

	private static void correlation(CorrelationType type, DataFile file) {
		correlation(type, file, Optional.empty());
	}

	private static void correlation(CorrelationType type, DataFile file, Optional<Aggregator> aggregator) {
		logCorrelationStart(type, file);
		CorrelationManager manager = managers.get(type);
		aggregator.ifPresent(agg -> ((PearsonMultiCorrelationManager) manager).updateAggregator(agg));
		manager.calculateCorrelations(file).forEach(Application::logCorrelation);
	}

	private static void logCorrelationStart(CorrelationType type, DataFile file) {
		System.out.println();
		System.out.println(String.format("Calculating %s correlation of data from %s", type, file));
	}

	private static void logCorrelation(Tuple2<CountryCollection, Double> tuple) {
		System.out.println(String.format("%s, Correlation: %.5f", tuple._1(), tuple._2()));
	}
}
