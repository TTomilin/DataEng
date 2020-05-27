package statistics.mapper.combinations;

import schema.entry.DataEntry;

public class SpearmanCombinationGenerator extends CombinationGenerator {

	public SpearmanCombinationGenerator(int combinationLength) {
		super(combinationLength);
	}

	@Override
	protected double getMeasureFromEnergyData(DataEntry data) {
		return data.getRank();
	}
}
