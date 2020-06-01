package statistics.mapper;

import java.util.Iterator;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.api.java.function.FlatMapFunction;

import scala.Tuple2;
import schema.entry.DataEntry;

/**
 * Rank the data incrementally using an atomic integer
 */
public class SpearmanIncrementalRanker implements FlatMapFunction<Tuple2<String, Iterable<DataEntry>>, DataEntry> {

	@Override
	public Iterator<DataEntry> call(Tuple2<String, Iterable<DataEntry>> tuples) {
		AtomicInteger count = new AtomicInteger();
		Iterable<DataEntry> dataEntries = tuples._2();
		dataEntries.forEach(data -> data.setRank(count.incrementAndGet()));
		return dataEntries.iterator();
	}
}
