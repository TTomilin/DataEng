package statistics;

import statistics.manager.*;

public enum CorrelationType {
	PEARSON			(PearsonCorrelationManager.class),
	SPEARMAN		(SpearmanCorrelationManager.class),
	TOTAL			(TotalCorrelationManager.class),
	PEARSON_MULTI	(PearsonMultiCorrelationManager.class),
	SPEARMAN_MULTI 	(SpearmanMultiCorrelationManager.class);

	private Class<? extends CorrelationManager> managerClass;

	CorrelationType(Class<? extends CorrelationManager> managerClass) {
		this.managerClass = managerClass;
	}

	public Class<? extends CorrelationManager> getManager() {
		return managerClass;
	}
}
