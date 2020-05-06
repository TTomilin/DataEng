package statistics.mapper;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.IterableUtils;
import org.apache.spark.api.java.function.Function;
import org.paukov.combinatorics.CombinatoricsFactory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import schema.CountryPair;
import schema.EnergyData;
import schema.EnergyDataPair;
import schema.EnergyValuePair;
import schema.RankedEnergyData;

/**
 * Maps the given collection of RankedEnergyData into a collection of combinations of every country pair
 */
public class NewCombinationGenerator implements Function<Iterable<RankedEnergyData>, Iterable<EnergyDataPair>> {

	public static final int COMBINATIONS_LENGTH = 2;

	/**
	 * @param energyData
	 * @return
	 */
	@Override
	public Iterable<EnergyDataPair> call(Iterable<RankedEnergyData> energyData) {
		Collection<RankedEnergyData> objects = IterableUtils.toList(energyData);
		return generateCombinations(objects).stream()
				.map(this::toValuePair)
				.collect(Collectors.toList());
	}

	/**
	 * Generates combinations of size COMBINATIONS_LENGTH from the list of energy data
	 * @param energyDataList
	 * @return
	 */
	private List<ICombinatoricsVector<EnergyData>> generateCombinations(Collection<RankedEnergyData> energyDataList) {
		ICombinatoricsVector<EnergyData> initialVector = CombinatoricsFactory.createVector(energyDataList);
		Generator<EnergyData> generator = CombinatoricsFactory.createSimpleCombinationGenerator(initialVector, COMBINATIONS_LENGTH);
		return generator.generateAllObjects();
	}

	/**
	 * Converts the given combinatorics vector into an energy data pair
	 * @param vector
	 * @return
	 */
	private EnergyDataPair toValuePair(ICombinatoricsVector<EnergyData> vector) {
		EnergyData firstValue = vector.getValue(0);
		EnergyData secondValue = vector.getValue(1);
		return new EnergyDataPair(firstValue.getTimestamp(),
				new CountryPair(firstValue.getCountry(), secondValue.getCountry()),
				new EnergyValuePair(firstValue.getValue(), secondValue.getValue()));
	}
}
