package nl.dataeng.tue.statistics.mapper;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;

import nl.dataeng.tue.schema.entry.DataEntry;
import org.apache.spark.sql.types.StructField;
import scala.Function1;
import scala.runtime.AbstractFunction1;

/**
 * Maps given Spark SQL Row into an DataEntry entry for better internal representation
 */
public class EnergyDataConverter implements FlatMapFunction<Row, DataEntry> {

	@Override
	public Iterator<DataEntry> call(Row row) {
		List<DataEntry> energyEntries = new ArrayList<>();
		Timestamp timestamp = row.getTimestamp(0);
		Function1<StructField,DataEntry> lambdaFunction = new AbstractFunction1<StructField, DataEntry>() {
			public DataEntry apply(StructField field){
				String countryCode = field.name();
				Double value = row.getAs(countryCode);
				DataEntry dataEntry = new DataEntry(timestamp, countryCode, value);
				energyEntries.add(dataEntry);
				return dataEntry;
			}
		};
		row.schema().toList().drop(1).foreach(lambdaFunction);
//				foreach(field -> { // Drop timestamp and iterate over fields
//			String countryCode = field.name();
//			Double value = row.getAs(countryCode);
//			DataEntry dataEntry = new DataEntry(timestamp, countryCode, value);
//			energyEntries.add(dataEntry);
//			return dataEntry;
//		});
		return energyEntries.iterator();
	}
}
