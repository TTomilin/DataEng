package session;

import javax.inject.Singleton;

import org.apache.spark.SparkConf;
import org.apache.spark.sql.SparkSession;

@Singleton
public class SessionWrapper {

	private static final String APP_NAME = "DataEng";
	private static SparkSession session;

	public static SparkSession getSession() {
		if (session == null) {
			session = SparkSession.builder()
					.appName(APP_NAME)
					.config(getSparkConfig())
					.getOrCreate();
		}
		return session;
	}

	private static SparkConf getSparkConfig() {
		return new SparkConf()
				.setAppName("DataEng")
				.setMaster("local[*]")
				.set("spark.executor.memory", "2g");
	}

	public static void close() {
		session.close();
	}
}
