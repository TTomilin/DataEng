package statistics.mapper.combinations;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.IterableUtils;
import org.paukov.combinatorics.CombinatoricsFactory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import lombok.RequiredArgsConstructor;
import scala.Tuple2;
import schema.entry.DataEntry;
import schema.entry.DataEntryCollection;
import schema.entry.DataEntrySet;

@RequiredArgsConstructor
public class TotalCombinationGenerator extends CombinationGenerator {

	private final int combinationLength;

	@Override
	public Integer getCombinationLength() {
		return combinationLength;
	}

	@Override
	public Iterator<DataEntryCollection> call(Tuple2<Timestamp, Iterable<DataEntry>> tuple) {
		Collection<DataEntry> dataEntries = IterableUtils.toList(tuple._2());
		return generateCombinations(dataEntries).stream()
				.map(ICombinatoricsVector::getVector)
				.map(this::toEntrySet)
				.collect(Collectors.toUnmodifiableSet())
				.iterator();
	}

	@Override
	protected double getMeasureFromEnergyData(DataEntry data) {
		return data.getValue();
	}

	private DataEntryCollection toEntrySet(List<DataEntry> dataEntries) {
		DataEntrySet entrySet = new DataEntrySet();
		entrySet.addAll(dataEntries);
		return entrySet;
	}

	public List<ICombinatoricsVector<DataEntry>> generateCombinations(Collection<DataEntry> dataEntries) {
		ICombinatoricsVector<DataEntry> initialVector = CombinatoricsFactory.createVector(dataEntries);
		Generator<DataEntry> generator = CombinatoricsFactory.createSimpleCombinationGenerator(initialVector, getCombinationLength());
		return generator.generateAllObjects();
	}
}
