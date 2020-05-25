package statistics.mapper;

import java.util.HashMap;
import java.util.Map;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import schema.CountryCollection;
import schema.CountrySet;
import schema.DataEntrySet;

public class TotalCountryPairWrapper implements PairFunction<Tuple2<DataEntrySet, Integer>, CountryCollection, Map<DataEntrySet, Integer>> {

	@Override
	public Tuple2<CountryCollection, Map<DataEntrySet, Integer>> call(Tuple2<DataEntrySet, Integer> tuple) {
		Map<DataEntrySet, Integer> counts = new HashMap<>();
		DataEntrySet entries = tuple._1();
		counts.put(entries, tuple._2());
		CountrySet countrySet = new CountrySet();
		countrySet.addAll(entries.getCountries());
		return new Tuple2<>(countrySet, counts);
	}
}
