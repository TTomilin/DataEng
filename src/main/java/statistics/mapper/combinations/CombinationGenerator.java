package statistics.mapper.combinations;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.IterableUtils;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.paukov.combinatorics.CombinatoricsFactory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import lombok.RequiredArgsConstructor;
import scala.Tuple2;
import schema.CorrelationMeasurePair;
import schema.entry.DataEntry;
import schema.entry.DataEntryCollection;
import schema.entry.DataEntryPair;
import schema.country.MultiCountryPair;

/**
 * Maps the given input Row into a collection of combinations of every country pair
 */
@RequiredArgsConstructor
public abstract class CombinationGenerator implements FlatMapFunction<Tuple2<Timestamp, Iterable<DataEntry>>, DataEntryCollection> {

	protected final int combinationLength;

	@Override
	public Iterator<DataEntryCollection> call(Tuple2<Timestamp, Iterable<DataEntry>> tuple) {
		Collection<DataEntry> dataEntries = IterableUtils.toList(tuple._2());
		return generateCombinations(dataEntries).stream()
				.map(this::toDataEntryPairs)
				.flatMap(Collection::stream)
				.collect(Collectors.toList())
				.iterator();
	}

	/**
	 * Override to determine the metric of the correlation
	 * @param data
	 * @return
	 */
	protected abstract double getMeasureFromEnergyData(DataEntry data);

	/**
	 * Generates combinations from the list of energy data of size depicted by the implementing class
	 * @param dataEntries
	 * @return
	 */
	public List<ICombinatoricsVector<DataEntry>> generateCombinations(Collection<DataEntry> dataEntries) {
		ICombinatoricsVector<DataEntry> initialVector = CombinatoricsFactory.createVector(dataEntries);
		Generator<DataEntry> generator = CombinatoricsFactory.createSimpleCombinationGenerator(initialVector, combinationLength);
		return generator.generateAllObjects();
	}

	/**
	 * Converts the given combinatorics vector into a collection of a single energy data pair
	 * @param vector
	 * @return
	 */
	protected Collection<DataEntryCollection> toDataEntryPairs(ICombinatoricsVector<DataEntry> vector) {
		DataEntry firstEntry = vector.getValue(0);
		DataEntry secondEntry = vector.getValue(1);
		MultiCountryPair multiCountryPair = new MultiCountryPair(Set.of(firstEntry.getCountry()), Set.of(secondEntry.getCountry()));
		CorrelationMeasurePair valuePair = new CorrelationMeasurePair(getMeasureFromEnergyData(firstEntry), getMeasureFromEnergyData(secondEntry));
		return Set.of(new DataEntryPair(multiCountryPair, valuePair));
	}
}
