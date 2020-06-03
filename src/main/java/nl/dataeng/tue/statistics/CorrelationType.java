package nl.dataeng.tue.statistics;

import nl.dataeng.tue.statistics.manager.CorrelationManager;
import nl.dataeng.tue.statistics.manager.PearsonCorrelationManager;
import nl.dataeng.tue.statistics.manager.PearsonMultiCorrelationManager;
import nl.dataeng.tue.statistics.manager.SpearmanCorrelationManager;
import nl.dataeng.tue.statistics.manager.TotalCorrelationManager;

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
