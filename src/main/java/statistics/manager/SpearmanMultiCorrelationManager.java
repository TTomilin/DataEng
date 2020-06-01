package statistics.manager;

import statistics.Aggregator;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.combinations.SpearmanMultiCombinationGenerator;

import static statistics.Aggregator.AVG;


public class SpearmanMultiCorrelationManager extends SpearmanCorrelationManager {

    private static final Aggregator DEFAULT_AGGREGATOR = AVG;

    private final SpearmanMultiCombinationGenerator generator;

    public SpearmanMultiCorrelationManager(Integer combinationLength) {
        super(combinationLength);
        this.generator = new SpearmanMultiCombinationGenerator(combinationLength, DEFAULT_AGGREGATOR);
    }

    @Override
    protected CombinationGenerator getCombinationGenerator() {
        return generator;
    }

    public void updateAggregator(Aggregator aggregator) {
        generator.setAggregator(aggregator);
    }
}
