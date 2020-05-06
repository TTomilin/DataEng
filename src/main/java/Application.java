import java.util.Arrays;

import data.DataFile;
import scala.Tuple2;
import schema.CountryPair;
import statistics.PearsonCorrelationManager;
import statistics.SpearmanCorrelationManager;

import static data.DataFile.SOLAR;
import static data.DataFile.WIND;

public class Application {

	private static PearsonCorrelationManager pearson = new PearsonCorrelationManager();
	private static SpearmanCorrelationManager spearman = new SpearmanCorrelationManager();

	public static void main(String[] args) {
		setHadoopHome(args);
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

	private static void spearmanCorrelation(DataFile type) {
		spearman.calculateCorrelations(type).forEach(Application::logCorrelation);
	}

	private static void pearsonCorrelation(DataFile type) {
		pearson.calculateCorrelations(type).forEach(Application::logCorrelation);
	}

	private static void logCorrelation(Tuple2<CountryPair, Double> tuple) {
		System.out.println(String.format("%s, Correlation: %.5f", tuple._1(), tuple._2()));
	}
}
