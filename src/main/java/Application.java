import java.util.Arrays;

import statistics.StatisticsManager;

public class Application {

	public static void main(String[] args) {
		// Add the location of hadoop binaries as a program argument if the JVM cannot find the HADOOP_HOME
		if (!Arrays.asList(args).isEmpty()) {
			System.setProperty("hadoop.home.dir", args[0]);
		}
		StatisticsManager manager = new StatisticsManager();
		manager.pearsonCorrelation();
	}
}
