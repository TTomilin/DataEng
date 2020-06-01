package statistics.manager;

import statistics.Aggregator;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.combinations.PearsonMultiCombinationGenerator;

import static statistics.Aggregator.AVG;

public class PearsonMultiCorrelationManager extends PearsonCorrelationManager {

	private static final Aggregator DEFAULT_AGGREGATOR = AVG;

	private final PearsonMultiCombinationGenerator generator;

	public PearsonMultiCorrelationManager(Integer combinationLength) {
		super(combinationLength);
		this.generator = new PearsonMultiCombinationGenerator(combinationLength, DEFAULT_AGGREGATOR);
	}

	@Override
	protected CombinationGenerator getCombinationGenerator() {
		return generator;
	}

	public void updateAggregator(Aggregator aggregator) {
		generator.setAggregator(aggregator);
	}
}
