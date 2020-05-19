package statistics;

import statistics.manager.CorrelationManager;
import statistics.manager.PearsonCorrelationManager;
import statistics.manager.PearsonMultiCorrelationManager;
import statistics.manager.SpearmanCorrelationManager;
import statistics.manager.TotalCorrelationManager;

public enum CorrelationType {
	PEARSON			(new PearsonCorrelationManager()),
	SPEARMAN		(new SpearmanCorrelationManager()),
	TOTAL			(new TotalCorrelationManager()),
	PEARSON_MULTI	(new PearsonMultiCorrelationManager());

	private CorrelationManager manager;

	CorrelationType(CorrelationManager manager) {
		this.manager = manager;
	}

	public CorrelationManager getManager() {
		return manager;
	}
}
