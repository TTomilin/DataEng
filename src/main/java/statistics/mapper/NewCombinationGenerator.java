package statistics.mapper;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.IterableUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.paukov.combinatorics.CombinatoricsFactory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import scala.Tuple2;
import schema.CountryPair;
import schema.EnergyDataPair;
import schema.EnergyValuePair;
import schema.RankedEnergyData;

/**
 * Maps the given collection of RankedEnergyData into a collection of combinations of every country pair
 */
public class NewCombinationGenerator implements FlatMapFunction<Tuple2<Timestamp, Iterable<RankedEnergyData>>, EnergyDataPair> {

	public static final int COMBINATIONS_LENGTH = 2;

	/**
	 * @param tuple
	 * @return
	 */
	@Override
	public Iterator<EnergyDataPair> call(Tuple2<Timestamp, Iterable<RankedEnergyData>> tuple) {
		Collection<RankedEnergyData> objects = IterableUtils.toList(tuple._2());
		return generateCombinations(objects).stream()
				.map(this::toValuePair)
				.collect(Collectors.toList())
				.iterator();
	}

	/**
	 * Generates combinations of size COMBINATIONS_LENGTH from the list of energy data
	 * @param energyDataList
	 * @return
	 */
	private List<ICombinatoricsVector<RankedEnergyData>> generateCombinations(Collection<RankedEnergyData> energyDataList) {
		ICombinatoricsVector<RankedEnergyData> initialVector = CombinatoricsFactory.createVector(energyDataList);
		Generator<RankedEnergyData> generator = CombinatoricsFactory.createSimpleCombinationGenerator(initialVector, COMBINATIONS_LENGTH);
		return generator.generateAllObjects();
	}

	/**
	 * Converts the given combinatorics vector into an energy data pair
	 * @param vector
	 * @return
	 */
	private EnergyDataPair toValuePair(ICombinatoricsVector<RankedEnergyData> vector) {
		RankedEnergyData firstEntry = vector.getValue(0);
		RankedEnergyData secondEntry = vector.getValue(1);
		return new EnergyDataPair(firstEntry.getTimestamp(),
				new CountryPair(firstEntry.getCountry(), secondEntry.getCountry()),
				new EnergyValuePair(firstEntry.getRank(), secondEntry.getRank()));
	}
}
