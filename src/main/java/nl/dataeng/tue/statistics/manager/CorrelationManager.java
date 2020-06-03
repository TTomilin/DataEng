package nl.dataeng.tue.statistics.manager;

import java.util.Collection;
import java.util.List;

import com.google.inject.internal.asm.$AnnotationVisitor;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.sql.Row;

import nl.dataeng.tue.data.DataFile;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import scala.Serializable;
import scala.Tuple2;
import nl.dataeng.tue.schema.country.CountryCollection;
import nl.dataeng.tue.schema.entry.DataEntry;
import nl.dataeng.tue.schema.entry.DataEntryCollection;
import nl.dataeng.tue.session.SessionWrapper;
import nl.dataeng.tue.statistics.mapper.EnergyDataConverter;
import nl.dataeng.tue.statistics.mapper.combinations.CombinationGenerator;
import nl.dataeng.tue.statistics.mapper.computation.StatisticComputer;
import nl.dataeng.tue.statistics.mapper.separation.FormulaSeparator;
import nl.dataeng.tue.statistics.mapper.wrapper.CountryPairWrapper;
import nl.dataeng.tue.statistics.reducer.FormulaComponentAggregator;
import nl.dataeng.tue.statistics.reducer.FormulaComponentSummator;

@Getter
@RequiredArgsConstructor
public abstract class CorrelationManager implements ICorrelationManager, Serializable {

	protected static final double THRESHOLD = 0.5;
	protected static final Integer DEFAULT_COMBINATION_LENGTH = 2;
	protected static final String PATH_TEMPLATE = "data/__default__/user/current/%s.csv";
//	protected static final String PATH_TEMPLATE = "%s.csv";

	protected FormulaComponentSummator componentSummator = new FormulaComponentSummator();
	protected CountryPairWrapper countryPairWrapper = new CountryPairWrapper();
	protected FormulaComponentAggregator componentAggregator = new FormulaComponentAggregator();
	protected EnergyDataConverter converter; // = new EnergyDataConverter();

	protected final Integer combinationLength;

	@Override
	public Collection<Tuple2<CountryCollection, Double>> calculateCorrelations(DataFile dataFile) {
		JavaRDD<DataEntry> javaRDD = getDataEntryJavaRDD(dataFile);
		JavaRDD<DataEntryCollection> combinationsJavaRDD = applyRanking(javaRDD)
				.groupBy(DataEntry::getTimestamp)
				.flatMap(getCombinationGenerator());

		assert combinationsJavaRDD != null;
		List<Tuple2<CountryCollection, Double>> var = getStatistics(combinationsJavaRDD)
//				.filter(this::applyThreshold)
				.collect();
		System.out.println("text");
		return var;
	}

	protected abstract JavaRDD<DataEntry> applyRanking(JavaRDD<DataEntry> javaRDD);
	protected abstract CombinationGenerator getCombinationGenerator();
	protected abstract FormulaSeparator getFormulaSeparator();
	protected abstract StatisticComputer getStatisticComputer();

	protected JavaRDD<DataEntry> getDataEntryJavaRDD(DataFile dataFile) {
		return SessionWrapper.getSession().read()
				.format("csv")
				.option("header", "true")
				.option("inferSchema", "true")
				.load(String.format(PATH_TEMPLATE, dataFile.getFileName()))
				.filter((FilterFunction<Row>) row -> !row.anyNull()) // Prevent Spark from reading superfluous rows
				.javaRDD()
				.flatMap(converter)
				.filter(this::valueProvided);
	}

	protected boolean valueProvided(DataEntry data) {
		return data.getValue() != 0;
	}

	protected JavaPairRDD<CountryCollection, Double> getStatistics(JavaRDD<DataEntryCollection> combinationsJavaRDD) {
		return combinationsJavaRDD
				.flatMapToPair(getFormulaSeparator())
				.reduceByKey(componentSummator)
				.mapToPair(countryPairWrapper)
				.reduceByKey(componentAggregator)
				.mapValues(getStatisticComputer());
	}

	protected boolean applyThreshold(Tuple2<CountryCollection, Double> tuple) {
		return tuple._2() >= THRESHOLD;
	}
}
