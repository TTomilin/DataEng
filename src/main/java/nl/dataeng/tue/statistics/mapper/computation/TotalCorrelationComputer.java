package nl.dataeng.tue.statistics.mapper.computation;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.commons.collections4.IterableUtils;

import nl.dataeng.tue.schema.entry.DataEntry;
import nl.dataeng.tue.schema.entry.DataEntryCollection;
import nl.dataeng.tue.schema.entry.DataEntrySet;

import static java.lang.Math.log10;

/**
 * Computes the correlation statistic given a map of respectively summed up formula components
 * Utilizes the Total correlation formula
 */
public class TotalCorrelationComputer extends CorrelationComputer<Iterable<DataEntryCollection>> {

	private static final int BIN_SIZE = 10;

	@Override
	public Double call(Iterable<DataEntryCollection> dataEntrySets) {
		Collection<DataEntrySet> dataEntries = IterableUtils.toList(dataEntrySets).stream()
				.map(DataEntrySet.class::cast)
				.collect(Collectors.toSet());
		int rowCount = dataEntries.stream().mapToInt(DataEntrySet::getCount).sum();
		Map<String, double[]> marginalProbabilities = getMarginalProbabilities(dataEntries, rowCount);
		return dataEntries.stream()
				.mapToDouble(entries -> singleJointCombination(marginalProbabilities, rowCount, entries))
				.sum();
	}

	private double singleJointCombination(Map<String, double[]> marginalProbabilityMap, double rowCount, DataEntrySet entries) {
		double jointProbability = entries.getCount() / rowCount;
		double marginalProbabilities = entries.stream()
				.map(entry -> marginalProbabilityMap.get(entry.getCountry())[(int) entry.getValue()])
				.map(BigDecimal::valueOf)
				.reduce(BigDecimal.ONE, BigDecimal::multiply)
				.doubleValue();
		return jointProbability * log2(jointProbability / marginalProbabilities);
	}

	private double log2(double x) {
		return log10(x) / log10(2);
	}

	private Map<String, double[]> getMarginalProbabilities(Collection<DataEntrySet> entrySets, double rowCount) {
		Map<String, double[]> marginals = entrySets.stream()
				.flatMap(Set::stream)
				.map(DataEntry::getCountry)
				.distinct()
				.collect(Collectors.toMap(java.util.function.Function.identity(), t -> new double[BIN_SIZE]));

		entrySets.stream().forEach(entrySet -> entrySet.stream()
				.forEach(entry -> updateMargin(rowCount, marginals, entrySet, entry)));
		return marginals;
	}

	private void updateMargin(double rowCount, Map<String, double[]> marginalProbabilities, DataEntrySet entrySet, DataEntry entry) {
		double[] probabilities = marginalProbabilities.get(entry.getCountry());
		probabilities[(int) entry.getValue()] += entrySet.getCount() / rowCount;
		marginalProbabilities.replace(entry.getCountry(), probabilities);
	}
}
