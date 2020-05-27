package statistics.mapper.combinations;

import schema.entry.DataEntry;

public class PearsonCombinationGenerator extends CombinationGenerator {

	public PearsonCombinationGenerator(int combinationLength) {
		super(combinationLength);
	}

	@Override
	protected double getMeasureFromEnergyData(DataEntry data) {
		return data.getValue();
	}
}
