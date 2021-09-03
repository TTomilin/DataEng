package statistics.mapper.combinations;

import java.sql.Timestamp;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.collections4.IterableUtils;
import org.paukov.combinatorics.ICombinatoricsVector;

import scala.Tuple2;
import schema.entry.DataEntry;
import schema.entry.DataEntryCollection;
import schema.entry.DataEntrySet;

public class TotalCombinationGenerator extends CombinationGenerator {

	public TotalCombinationGenerator(int combinationLength) {
		super(combinationLength);
	}

	@Override
	public Iterator<DataEntryCollection> call(Tuple2<Timestamp, Iterable<DataEntry>> tuple) {
		Collection<DataEntry> dataEntries = IterableUtils.toList(tuple._2());
		return generateCombinations(dataEntries).stream()
				.map(ICombinatoricsVector::getVector)
				.map(this::toEntrySet)
				.collect(Collectors.toSet())
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
}
