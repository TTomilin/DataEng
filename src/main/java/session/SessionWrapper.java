package session;

import javax.inject.Singleton;

import org.apache.spark.sql.SparkSession;

@Singleton
public class SessionWrapper {

	private static SparkSession session;

	public static SparkSession getSession() {
		if (session == null) {
			session = SparkSession.builder()
					.appName("DataEng")
					.config("spark.master", "local")
					.getOrCreate();
		}
		return session;
	}

	public static void close() {
		session.close();
	}
}
