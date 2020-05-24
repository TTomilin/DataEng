package statistics.mapper.combinations;

import schema.DataEntry;

public class TotalCombinationGenerator extends CombinationGenerator {

	private static final int COMBINATION_LENGTH = 3;

	@Override
	protected double getMeasureFromEnergyData(DataEntry data) {
		return data.getValue();
	}

	@Override
	protected Integer getCombinationLength() {
		return COMBINATION_LENGTH;
	}
}
