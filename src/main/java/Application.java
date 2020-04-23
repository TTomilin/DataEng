import session.SessionWrapper;
import statistics.StatisticsManager;

public class Application {

	public static void main(String[] args) {
		StatisticsManager manager = new StatisticsManager();
		manager.correlation();
		SessionWrapper.close();
	}
}
