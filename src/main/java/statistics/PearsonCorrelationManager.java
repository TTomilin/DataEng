package statistics;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Row;

import schema.EnergyDataPair;
import statistics.mapper.CombinationGenerator;
import statistics.mapper.FormulaSeparator;
import statistics.mapper.PearsonFormulaSeparator;
import statistics.mapper.PearsonStatisticComputer;
import statistics.mapper.StatisticComputer;

public class PearsonCorrelationManager extends CorrelationManager {

	private FormulaSeparator separator = new PearsonFormulaSeparator();
	private StatisticComputer computer = new PearsonStatisticComputer();

	@Override
	protected JavaRDD<EnergyDataPair> preprocess(JavaRDD<Row> rdd) {
		return rdd.flatMap(new CombinationGenerator()).filter(this::bothValuesProvided);
	}

	@Override
	protected FormulaSeparator getFormulaSeparator() {
		return separator;
	}

	@Override
	protected StatisticComputer getStatisticComputer() {
		return computer;
	}

	private boolean bothValuesProvided(EnergyDataPair pair) {
		return pair.getEnergyValuePair().bothValuesPresent();
	}
}
