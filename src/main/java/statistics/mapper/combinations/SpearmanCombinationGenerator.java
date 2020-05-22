package statistics.mapper.combinations;

import schema.DataEntry;

public class SpearmanCombinationGenerator extends CombinationGenerator {

	private static final int COMBINATION_LENGTH = 2;

	@Override
	protected double getMeasureFromEnergyData(DataEntry data) {
		return data.getRank();
	}

	@Override
	protected Integer getCombinationLength() {
		return COMBINATION_LENGTH;
	}
}
