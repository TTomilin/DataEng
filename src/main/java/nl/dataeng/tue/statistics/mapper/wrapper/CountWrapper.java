package nl.dataeng.tue.statistics.mapper.wrapper;

import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import nl.dataeng.tue.schema.entry.DataEntryCollection;

/**
 * Wraps the collection of nl.dataeng.tue.data entries input into a
 * tuple as preparation for further counting by key
 */
public class CountWrapper implements PairFunction<DataEntryCollection, DataEntryCollection, Integer> {

	@Override
	public Tuple2<DataEntryCollection, Integer> call(DataEntryCollection entries) {
		return new Tuple2<>(entries, 1);
	}
}
