package statistics;

import statistics.manager.CorrelationManager;
import statistics.manager.PearsonCorrelationManager;
import statistics.manager.SpearmanCorrelationManager;

public enum CorrelationType {
	PEARSON	(new PearsonCorrelationManager()),
	SPEARMAN(new SpearmanCorrelationManager());

	private CorrelationManager manager;

	CorrelationType(CorrelationManager manager) {
		this.manager = manager;
	}

	public CorrelationManager getManager() {
		return manager;
	}
}
