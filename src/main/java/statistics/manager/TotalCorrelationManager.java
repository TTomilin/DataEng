package statistics.manager;

import org.apache.spark.api.java.JavaRDD;

import schema.DataEntry;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.computation.StatisticComputer;
import statistics.mapper.separation.FormulaSeparator;

public class TotalCorrelationManager extends CorrelationManager {

	@Override
	protected JavaRDD<DataEntry> applyRanking(JavaRDD<DataEntry> javaRDD) {
		return null;
	}

	@Override
	protected CombinationGenerator getCombinationGenerator() {
		return null;
	}

	@Override
	protected FormulaSeparator getFormulaSeparator() {
		return null;
	}

	@Override
	protected StatisticComputer getStatisticComputer() {
		return null;
	}
}
