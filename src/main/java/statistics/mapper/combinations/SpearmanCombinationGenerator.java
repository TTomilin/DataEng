package statistics.mapper.combinations;

import schema.DataEntry;

public class SpearmanCombinationGenerator extends CombinationGenerator {

	@Override
	protected double getMeasureFromEnergyData(DataEntry data) {
		return data.getRank();
	}
}
