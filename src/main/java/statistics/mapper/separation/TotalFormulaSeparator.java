package statistics.mapper.separation;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import schema.DataEntrySet;

/**
 * FormulaSeparator implementation for the Total correlation
 */
public class TotalFormulaSeparator implements PairFunction<DataEntrySet, DataEntrySet, Integer> {

	@Override
	public Tuple2<DataEntrySet, Integer> call(DataEntrySet entries) {
//		int[][][] counts = new int[BIN_SIZE][BIN_SIZE][BIN_SIZE];
//		DataEntryCollection[][][] counts = new DataEntryCollection[BIN_SIZE][BIN_SIZE][BIN_SIZE];
//		Set<@NonNull String> countries = dataEntries.stream().map(DataEntry::getCountry).collect(Collectors.toSet());
//		DataEntryCollection occurrence = new DataEntryCollection(countries, 1);
//		Map<Collection<DataEntry>, Integer> occurrences = new HashMap<>();
//		occurrences.put(dataEntries, 1);
		return new Tuple2<>(entries, 1);
	}
}
