package statistics.mapper;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import schema.entry.DataEntry;

/**
 * Maps given Spark SQL Row into an DataEntry entry for better internal representation
 */
public class EnergyDataConverter implements FlatMapFunction<Row, DataEntry> {

	@Override
	public Iterator<DataEntry> call(Row row) {
		List<DataEntry> energyEntries = new ArrayList<>();
		Timestamp timestamp = row.getTimestamp(0);
		row.schema().toList().drop(1).foreach(field -> { // Drop timestamp and iterate over fields
			String countryCode = field.name();
			Double value = row.getAs(countryCode);
			DataEntry dataEntry = new DataEntry(timestamp, countryCode, value);
			energyEntries.add(dataEntry);
			return dataEntry;
		});
		return energyEntries.iterator();
	}
}
