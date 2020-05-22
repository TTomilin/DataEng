package statistics.manager;

import statistics.Aggregator;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.combinations.PearsonMultiCombinationGenerator;

public class PearsonMultiCorrelationManager extends PearsonCorrelationManager {

	private final CombinationGenerator generator;

	public PearsonMultiCorrelationManager(Aggregator aggregator) {
		this.generator = new PearsonMultiCombinationGenerator(aggregator);
	}

	@Override
	protected CombinationGenerator getCombinationGenerator() {
		return generator;
	}
}
