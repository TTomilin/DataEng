package statistics.mapper;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import schema.DataEntrySet;

public class TotalCountryPairWrapper implements PairFunction<Tuple2<DataEntrySet, Integer>, Collection<String>, Map<Collection<Integer>, Integer>> {

	@Override
	public Tuple2<Collection<String>, Map<Collection<Integer>, Integer>> call(Tuple2<DataEntrySet, Integer> tuple) {
		Map<Collection<Integer>, Integer> counts = new HashMap<>();
		DataEntrySet entries = tuple._1();
		counts.put(entries.getValues(), 1);
		return new Tuple2<>(entries.getCountries(), counts);
	}
}
