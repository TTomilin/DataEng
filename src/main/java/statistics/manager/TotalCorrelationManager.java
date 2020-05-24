package statistics.manager;

import org.apache.spark.api.java.JavaRDD;

import schema.DataEntry;
import statistics.mapper.combinations.CombinationGenerator;
import statistics.mapper.combinations.TotalCombinationGenerator;
import statistics.mapper.computation.StatisticComputer;
import statistics.mapper.computation.TotalStatisticComputer;
import statistics.mapper.separation.FormulaSeparator;
import statistics.mapper.separation.TotalFormulaSeparator;

public class TotalCorrelationManager extends CorrelationManager {

	private CombinationGenerator generator = new TotalCombinationGenerator();
	private FormulaSeparator separator = new TotalFormulaSeparator();
	private StatisticComputer computer = new TotalStatisticComputer();

	@Override
	protected JavaRDD<DataEntry> applyRanking(JavaRDD<DataEntry> javaRDD) {
		return javaRDD;
	}

	@Override
	protected CombinationGenerator getCombinationGenerator() {
		return generator;
	}

	@Override
	protected FormulaSeparator getFormulaSeparator() {
		return separator;
	}

	@Override
	protected StatisticComputer getStatisticComputer() {
		return computer;
	}
}
