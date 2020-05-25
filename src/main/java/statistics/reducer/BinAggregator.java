package statistics.reducer;

import java.util.Collection;
import java.util.Map;

import org.apache.spark.api.java.function.Function2;

public class BinAggregator implements Function2<Map<Collection<Integer>, Integer>, Map<Collection<Integer>, Integer>, Map<Collection<Integer>, Integer>> {

	@Override
	public Map<Collection<Integer>, Integer> call(Map<Collection<Integer>, Integer> firstMap, Map<Collection<Integer>, Integer> secondMap) {
		firstMap.putAll(secondMap);
		return firstMap;
	}
}
