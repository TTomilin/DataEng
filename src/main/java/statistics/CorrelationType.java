package statistics;

import statistics.manager.CorrelationManager;
import statistics.manager.PearsonCorrelationManager;
import statistics.manager.PearsonMultiCorrelationManager;
import statistics.manager.SpearmanCorrelationManager;
import statistics.manager.TotalCorrelationManager;

public enum CorrelationType {
	PEARSON			(PearsonCorrelationManager.class),
	SPEARMAN		(SpearmanCorrelationManager.class),
	TOTAL			(TotalCorrelationManager.class),
	PEARSON_MULTI	(PearsonMultiCorrelationManager.class);

	private Class<? extends CorrelationManager> managerClass;

	CorrelationType(Class<? extends CorrelationManager> managerClass) {
		this.managerClass = managerClass;
	}

	public Class<? extends CorrelationManager> getManager() {
		return managerClass;
	}
}
