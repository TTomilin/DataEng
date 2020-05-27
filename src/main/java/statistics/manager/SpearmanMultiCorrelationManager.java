package statistics.manager;

import statistics.Aggregator;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.combinations.SpearmanMultiCombinationGenerator;


public class SpearmanMultiCorrelationManager extends SpearmanCorrelationManager {

    private final CombinationGenerator generator;

    public SpearmanMultiCorrelationManager(Aggregator aggregator) {
        this.generator = new SpearmanMultiCombinationGenerator(aggregator);
    }

    @Override
    protected CombinationGenerator getCombinationGenerator() {
        return generator;
    }
}
