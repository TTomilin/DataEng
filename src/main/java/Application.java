import java.util.Arrays;

import data.EnergyType;
import scala.Tuple2;
import schema.CountryPair;
import statistics.StatisticsManager;

import static data.EnergyType.SOLAR;
import static data.EnergyType.WIND;

public class Application {

	private static StatisticsManager manager;

	public static void main(String[] args) {
		setHadoopHome(args);
		manager = new StatisticsManager();
		pearsonCorrelation(WIND);
		pearsonCorrelation(SOLAR);
	}

	private static void setHadoopHome(String[] args) {
		// Add the location of hadoop binaries as a program argument if the JVM cannot find the HADOOP_HOME
		if (!Arrays.asList(args).isEmpty()) {
			System.setProperty("hadoop.home.dir", args[0]);
		}
	}

	private static void pearsonCorrelation(EnergyType type) {
		manager.pearsonCorrelations(type).forEach(Application::logCorrelation);
	}

	private static void logCorrelation(Tuple2<CountryPair, Double> tuple) {
		System.out.println(String.format("%s, Correlation: %.5f", tuple._1(), tuple._2()));
	}
}
