package statistics.mapper.computation;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;

import org.apache.spark.api.java.function.Function;

/**
 * Computes the correlation statistic given a map of respectively summed up formula components
 * Utilizes the Total correlation formula
 */
public class TotalStatisticComputer implements Function<Map<Collection<Integer>, Integer>, Double> {

	private static final int BIN_SIZE = 10;

	@Override
	public Double call(Map<Collection<Integer>, Integer> countMap) {
//		Map<Integer, Integer> marginalOccurrences = new HashMap<>();
		int[] marginalOccurrences = new int[BIN_SIZE];
		countMap.keySet().stream().flatMap(Collection::stream).forEach(value -> marginalOccurrences[value]++);
		countMap.forEach((key, val) -> log(key, val));
		for (int i = 0; i < BIN_SIZE; i++) {
			System.out.println(i + " occurs " + marginalOccurrences[i] + " times");
		}
		return null;
	}

	private void log(Collection<Integer> key, Integer value) {
		System.out.println("Values: " + key + ", Count: " + value);
	}
}
