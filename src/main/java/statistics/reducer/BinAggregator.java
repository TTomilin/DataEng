package statistics.reducer;

import java.util.Collection;
import java.util.Map;

import org.apache.spark.api.java.function.Function2;

import schema.DataEntrySet;

public class BinAggregator implements Function2<Map<DataEntrySet, Integer>, Map<DataEntrySet, Integer>, Map<DataEntrySet, Integer>> {

	@Override
	public Map<DataEntrySet, Integer> call(Map<DataEntrySet, Integer> firstMap, Map<DataEntrySet, Integer> secondMap) {
		System.out.println("Aggregating " + firstMap.keySet() + " with " + secondMap.keySet());
		firstMap.putAll(secondMap);
		return firstMap;
	}
}
