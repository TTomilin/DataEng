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
import schema.DataEntry;
import schema.DataEntryPair;
import schema.CorrelationMeasurePair;

/**
 * Maps the given input Row into a collection of combinations of every country pair
 */
public abstract class CombinationGenerator implements FlatMapFunction<Tuple2<Timestamp, Iterable<DataEntry>>, DataEntryPair> {

	private static final int COMBINATIONS_LENGTH = 2;

	/**
	 * @param tuple
	 * @return
	 */
	@Override
	public Iterator<DataEntryPair> call(Tuple2<Timestamp, Iterable<DataEntry>> tuple) {
		Collection<DataEntry> objects = IterableUtils.toList(tuple._2());
		return generateCombinations(objects).stream()
				.map(this::toValuePair)
				.collect(Collectors.toList())
				.iterator();
	}

	protected abstract double getMeasureFromEnergyData(DataEntry data);

	/**
	 * Generates combinations of size COMBINATIONS_LENGTH from the list of energy data
	 * @param dataEntryList
	 * @return
	 */
	private List<ICombinatoricsVector<DataEntry>> generateCombinations(Collection<DataEntry> dataEntryList) {
		ICombinatoricsVector<DataEntry> initialVector = CombinatoricsFactory.createVector(dataEntryList);
		Generator<DataEntry> generator = CombinatoricsFactory.createSimpleCombinationGenerator(initialVector, COMBINATIONS_LENGTH);
		return generator.generateAllObjects();
	}

	/**
	 * Converts the given combinatorics vector into an energy data pair
	 * @param vector
	 * @return
	 */
	private DataEntryPair toValuePair(ICombinatoricsVector<DataEntry> vector) {
		DataEntry firstEntry = vector.getValue(0);
		DataEntry secondEntry = vector.getValue(1);
		return new DataEntryPair(firstEntry.getTimestamp(),
				new CountryPair(
						firstEntry.getCountry(),
						secondEntry.getCountry()
				),
				new CorrelationMeasurePair(
						getMeasureFromEnergyData(firstEntry),
						getMeasureFromEnergyData(secondEntry)
				)
		);
	}
}
