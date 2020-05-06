package statistics;

import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import scala.Tuple2;
import schema.EnergyData;
import schema.EnergyDataPair;
import schema.RankedEnergyData;
import statistics.mapper.FormulaSeparator;
import statistics.mapper.NewCombinationGenerator;
import statistics.mapper.SpearmanFormulaSeparator;
import statistics.mapper.SpearmanStatisticComputer;
import statistics.mapper.StatisticComputer;

public class SpearmanCorrelationManager extends CorrelationManager {

	private static final int NUM_PARTITIONS = 10;

	private FormulaSeparator separator = new SpearmanFormulaSeparator();
	private StatisticComputer computer = new SpearmanStatisticComputer();

	@Override
	protected JavaRDD<EnergyDataPair> preprocess(JavaRDD<Row> rdd) {
		return rdd
				.flatMap(this::toEnergyValues)
				.filter(this::valueProvided)
				.sortBy(EnergyData::getValue, true, NUM_PARTITIONS)
				.groupBy(EnergyData::getCountry)
				.map(this::rank)
				.flatMap(data -> data._2().iterator())
				.groupBy(EnergyData::getTimestamp)
				.flatMap(new NewCombinationGenerator());
	}

	@Override
	protected FormulaSeparator getFormulaSeparator() {
		return separator;
	}

	@Override
	protected StatisticComputer getStatisticComputer() {
		return computer;
	}

	private boolean valueProvided(EnergyData data) {
		return data.getValue() != 0;
	}

	private Iterator<RankedEnergyData> toEnergyValues(Row row) {
		List<RankedEnergyData> energyEntries = new ArrayList<>();
		Timestamp timestamp = row.getTimestamp(0);
		row.schema().toList().drop(1).foreach(field -> { // Drop timestamp and iterate over fields
			String countryCode = field.name();
			Double value = row.getAs(countryCode);
			RankedEnergyData energyData = new RankedEnergyData(timestamp, countryCode, value);
			energyEntries.add(energyData);
			return energyData;
		});
		return energyEntries.iterator();
	}

	public Tuple2<String, Iterable<RankedEnergyData>> rank(Tuple2<String, Iterable<RankedEnergyData>> tuples) {
		AtomicInteger count = new AtomicInteger();
		tuples._2().forEach(data -> data.setRank(count.incrementAndGet()));
		return tuples;
	}
}
