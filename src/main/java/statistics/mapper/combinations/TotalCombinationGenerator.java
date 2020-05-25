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

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import scala.Tuple2;
import schema.CountrySet;
import schema.DataEntry;
import schema.DataEntryCollection;
import schema.DataEntrySet;

@Getter
@RequiredArgsConstructor
public class TotalCombinationGenerator implements FlatMapFunction<Tuple2<Timestamp, Iterable<DataEntry>>, DataEntrySet> {

	private final int combinationLength;

	@Override
	public Iterator<DataEntrySet> call(Tuple2<Timestamp, Iterable<DataEntry>> tuple) {
		Collection<DataEntry> dataEntries = IterableUtils.toList(tuple._2());
		return generateCombinations(dataEntries).stream()
				.map(ICombinatoricsVector::getVector)
				.map(this::toEntrySet)
//				.map(this::toEntryCollection)
//				.map(Collections::unmodifiableList) // HashMap key has to be immutable to avoid unsuspected behaviour
				.collect(Collectors.toUnmodifiableSet())
				.iterator();
	}

	private DataEntrySet toEntrySet(List<DataEntry> dataEntries) {
		DataEntrySet entrySet = new DataEntrySet();
		entrySet.addAll(dataEntries);
		return entrySet;
	}

	private DataEntryCollection toEntryCollection(List<DataEntry> dataEntries) {
		CountrySet countries = new CountrySet();
		dataEntries.stream().map(DataEntry::getCountry).forEach(countries::add);
		Set<Integer> values = dataEntries.stream().map(DataEntry::getValue).map(Double::intValue).collect(Collectors.toSet());
		return new DataEntryCollection(countries, values);
	}

	public List<ICombinatoricsVector<DataEntry>> generateCombinations(Collection<DataEntry> dataEntries) {
		ICombinatoricsVector<DataEntry> initialVector = CombinatoricsFactory.createVector(dataEntries);
		Generator<DataEntry> generator = CombinatoricsFactory.createSimpleCombinationGenerator(initialVector, getCombinationLength());
		return generator.generateAllObjects();
	}
}
