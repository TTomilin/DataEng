import java.util.Arrays;

import static org.apache.log4j.lf5.LogLevel.WARN;

import data.DataFile;
import scala.Tuple2;
import schema.CountryPair;
import session.SessionWrapper;
import statistics.CorrelationType;
import statistics.manager.PearsonCorrelationManager;
import statistics.manager.SpearmanCorrelationManager;

import static data.DataFile.SOLAR;
import static data.DataFile.WIND;
import static statistics.CorrelationType.PEARSON;
import static statistics.CorrelationType.SPEARMAN;

public class Application {

	private static PearsonCorrelationManager pearson = new PearsonCorrelationManager();
	private static SpearmanCorrelationManager spearman = new SpearmanCorrelationManager();

	public static void main(String[] args) {
		setHadoopHome(args);
		SessionWrapper.setLogLevel(WARN);
		pearsonCorrelation(WIND);
		spearmanCorrelation(WIND);
		pearsonCorrelation(SOLAR);
		spearmanCorrelation(SOLAR);
	}

	private static void setHadoopHome(String[] args) {
		// Add the location of hadoop binaries as a program argument if the JVM cannot find the HADOOP_HOME
		if (!Arrays.asList(args).isEmpty()) {
			System.setProperty("hadoop.home.dir", args[0]);
		}
	}

	private static void pearsonCorrelation(DataFile file) {
		logCorrelationStart(PEARSON, file);
		pearson.calculateCorrelations(file).forEach(Application::logCorrelation);
	}

	private static void spearmanCorrelation(DataFile file) {
		logCorrelationStart(SPEARMAN, file);
		spearman.calculateCorrelations(file).forEach(Application::logCorrelation);
	}

	private static void logCorrelationStart(CorrelationType type, DataFile file) {
		System.out.println();
		System.out.println(String.format("Calculating %s correlation of %s data", type, file));
	}

	private static void logCorrelation(Tuple2<CountryPair, Double> tuple) {
		System.out.println(String.format("%s, Correlation: %.5f", tuple._1(), tuple._2()));
	}
}
