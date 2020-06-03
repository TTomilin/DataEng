package nl.dataeng.tue.statistics.mapper.combinations;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.HashSet;
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
import nl.dataeng.tue.schema.CorrelationMeasurePair;
import nl.dataeng.tue.schema.country.MultiCountryPair;
import nl.dataeng.tue.schema.entry.DataEntry;
import nl.dataeng.tue.schema.entry.DataEntryCollection;
import nl.dataeng.tue.schema.entry.DataEntryPair;

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
	 * Generates combinations from the list of energy nl.dataeng.tue.data of size depicted by the implementing class
	 * @param dataEntries
	 * @return
	 */
	public List<ICombinatoricsVector<DataEntry>> generateCombinations(Collection<DataEntry> dataEntries) {
		ICombinatoricsVector<DataEntry> initialVector = CombinatoricsFactory.createVector(dataEntries);
		Generator<DataEntry> generator = CombinatoricsFactory.createSimpleCombinationGenerator(initialVector, combinationLength);
		return generator.generateAllObjects();
	}

	/**
	 * Converts the given combinatorics vector into a collection of a single energy nl.dataeng.tue.data pair
	 * @param vector
	 * @return
	 */
	protected Collection<DataEntryCollection> toDataEntryPairs(ICombinatoricsVector<DataEntry> vector) {
		DataEntry firstEntry = vector.getValue(0);
		DataEntry secondEntry = vector.getValue(1);
		Set<String> firstSet = new HashSet<>();
		Set<String> secondSet = new HashSet<>();
		firstSet.add(firstEntry.getCountry());
		secondSet.add(secondEntry.getCountry());
		MultiCountryPair multiCountryPair = new MultiCountryPair(firstSet, secondSet);
		CorrelationMeasurePair valuePair = new CorrelationMeasurePair(getMeasureFromEnergyData(firstEntry), getMeasureFromEnergyData(secondEntry));
		Collection<DataEntryCollection> pairSet = new HashSet<>();
		pairSet.add(new DataEntryPair(multiCountryPair, valuePair));
		return pairSet;
	}
}
