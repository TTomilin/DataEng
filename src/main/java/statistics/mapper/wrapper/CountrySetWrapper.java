package statistics.mapper.wrapper;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import schema.country.CountryCollection;
import schema.country.CountrySet;
import schema.entry.DataEntryCollection;
import schema.entry.DataEntrySet;

public class CountrySetWrapper implements PairFunction<Tuple2<DataEntryCollection, Integer>, CountryCollection, DataEntryCollection> {

	@Override
	public Tuple2<CountryCollection, DataEntryCollection> call(Tuple2<DataEntryCollection, Integer> tuple) {
		DataEntrySet dataEntrySet = (DataEntrySet) tuple._1();
		dataEntrySet.setCount(tuple._2());
		CountrySet countrySet = new CountrySet();
		countrySet.addAll(dataEntrySet.getCountries());
		return new Tuple2<>(countrySet, dataEntrySet);
	}
}
