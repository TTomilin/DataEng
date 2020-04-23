import session.SessionWrapper;
import statistics.StatisticsManager;

public class Application {

	public static void main(String[] args) {
		StatisticsManager manager = new StatisticsManager();
		manager.correlationFromData();
		manager.correlationFromFile();
		SessionWrapper.close();
	}
}
