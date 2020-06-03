package nl.dataeng.tue.statistics.manager;

import nl.dataeng.tue.statistics.Aggregator;
import nl.dataeng.tue.statistics.mapper.combinations.CombinationGenerator;
import nl.dataeng.tue.statistics.mapper.combinations.PearsonMultiCombinationGenerator;

import static nl.dataeng.tue.statistics.Aggregator.AVG;

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
