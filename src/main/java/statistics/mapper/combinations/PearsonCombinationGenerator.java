package statistics.mapper.combinations;

import schema.DataEntry;

public class PearsonCombinationGenerator extends CombinationGenerator {

	private static final int COMBINATION_LENGTH = 2;

	@Override
	protected double getMeasureFromEnergyData(DataEntry data) {
		return data.getValue();
	}

	@Override
	protected Integer getCombinationLength() {
		return COMBINATION_LENGTH;
	}
}
