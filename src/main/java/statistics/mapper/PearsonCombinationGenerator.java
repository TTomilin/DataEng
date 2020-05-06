package statistics.mapper;

import schema.DataEntry;

public class PearsonCombinationGenerator extends CombinationGenerator {

	@Override
	protected double getMeasureFromEnergyData(DataEntry data) {
		return data.getValue();
	}
}
