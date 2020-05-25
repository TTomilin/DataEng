package statistics.mapper.computation;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.Function;

import lombok.NonNull;
import schema.DataEntry;
import schema.DataEntrySet;

/**
 * Computes the correlation statistic given a map of respectively summed up formula components
 * Utilizes the Total correlation formula
 */
public class TotalStatisticComputer implements Function<Map<DataEntrySet, Integer>, Double> {

	private static final int BIN_SIZE = 10;

	@Override
	public Double call(Map<DataEntrySet, Integer> countMap) {
		countMap.forEach((key, val) -> log(key.getValues(), val));
//		int[][] marginalProbs = new int[countMap.size()][BIN_SIZE];
		Map<String, double[]> marginalProbabilities = new HashMap<>();

		countMap.keySet().stream().flatMap(Set::stream).map(DataEntry::getCountry).forEach(country -> marginalProbabilities.put(country, emptyArray(BIN_SIZE)));

		Iterator<Entry<DataEntrySet, Integer>> iterator = countMap.entrySet().iterator();
		while (iterator.hasNext()) {
			Entry<DataEntrySet, Integer> entry = iterator.next();
			DataEntrySet entrySet = entry.getKey();
			Iterator<DataEntry> entryIterator = entrySet.iterator();
			while (entryIterator.hasNext()) {
				DataEntry dataEntry = entryIterator.next();
				@NonNull String country = dataEntry.getCountry();
				double value = dataEntry.getValue();
				double[] probs = marginalProbabilities.get(country);
				probs[(int) value] += entry.getValue();
				marginalProbabilities.replace(country, probs);
			}
		}

		marginalProbabilities.replaceAll(this::divide);

//		countMap.keySet().stream().map(DataEntrySet::getValues).flatMap(Collection::stream).forEach(value -> marginalProbabilities[value]++);

		/*double[] marginalProbs = new double[BIN_SIZE];
		countMap.keySet().stream().map(DataEntrySet::getValues).map(this::toOccurrences).collect(Collectors.toSet());
		for (int i = 0; i < marginalProbs.length; i++) {
			marginalProbs[i] /= 10;
		}*/
//		Arrays.stream(marginalProbs).forEach();

		double result = 0;

		int totalCount = countMap.values().stream().mapToInt(i -> i).sum();

		Iterator<Entry<DataEntrySet, Integer>> entryIterator = countMap.entrySet().iterator();
		while (entryIterator.hasNext()) {
			Entry<DataEntrySet, Integer> entry = entryIterator.next();
			DataEntrySet dataEntries = entry.getKey();
			Integer count = entry.getValue();

			double jointProbability = count / (double) totalCount;

			Iterator<DataEntry> dataEntryIterator = dataEntries.iterator();
			double marginalProbabilityProduct = 1;
			while (dataEntryIterator.hasNext()) {
				DataEntry dataEntry = dataEntryIterator.next();
				@NonNull String country = dataEntry.getCountry();
				int value = (int) dataEntry.getValue();
				double prob = marginalProbabilities.get(country)[value];
				marginalProbabilityProduct *= prob;
			}
			double total = jointProbability * Math.log(jointProbability / marginalProbabilityProduct);
			result += total;
			System.out.println(String.format("Joint: %f, Marg: %f, Total: %f", jointProbability, marginalProbabilityProduct, total));
		}
		return result;
	}

	private double[] divide(String country, double[] occurrences) {
		return Arrays.stream(occurrences).map(value -> value /= BIN_SIZE).toArray();
	}

	private double[] emptyArray(int size) {
		double[] array = new double[size];
		Arrays.fill(array, 0.0);
		return array;
	}

	private int[] toOccurrences(List<Integer> values) {
		int[] marginalOccurrences = new int[BIN_SIZE];
		values.stream().forEach(value -> marginalOccurrences[value]++);
		return marginalOccurrences;
	}

	private void log(Collection<Integer> key, Integer value) {
		System.out.println("Values: " + key + ", Count: " + value);
	}
}
