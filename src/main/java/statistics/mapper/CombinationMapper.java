package statistics.mapper;

import java.sql.Timestamp;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Row;
import org.paukov.combinatorics.CombinatoricsFactory;
import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;

import schema.EnergyData;
import schema.EnergyDataPair;
import schema.EnergyValuePair;
import schema.CountryPair;

/**
 * Maps the given input Row into a collection of country-wise combinations
 */
public class CombinationMapper implements FlatMapFunction<Row, EnergyDataPair> {

	public static final int COMBINATIONS_LENGTH = 2;

	/**
	 * Callable function to override for FlatMapFunction implementation
	 * @param row
	 * @return
	 */
	@Override
	public Iterator<EnergyDataPair> call(Row row) {
		Set<EnergyData> energyDataList = toEnergyValues(row);
		return generateCombinations(energyDataList).stream()
				.map(this::toValuePair)
				.collect(Collectors.toList())
				.iterator();
	}

	/**
	 * Maps given Spark SQL Row into an EnergyData entry for better internal representation
	 * @param row
	 * @return
	 */
	private Set<EnergyData> toEnergyValues(Row row) {
		Set<EnergyData> energyEntries = new HashSet<>();
		Timestamp timestamp = row.getTimestamp(0);
		row.schema().toList().drop(1).foreach(field -> { // Drop timestamp and iterate over fields
			String countryCode = field.name();
			Double value = row.getAs(countryCode);
			EnergyData energyData = new EnergyData(timestamp, countryCode, value);
			energyEntries.add(energyData);
			return energyData;
		});
		return energyEntries;
	}

	/**
	 * Generates combinations of size COMBINATIONS_LENGTH from the list of energy data
	 * @param energyDataList
	 * @return
	 */
	private List<ICombinatoricsVector<EnergyData>> generateCombinations(Set<EnergyData> energyDataList) {
		ICombinatoricsVector<EnergyData> initialVector = CombinatoricsFactory.createVector(energyDataList);
		Generator<EnergyData> generator = CombinatoricsFactory.createSimpleCombinationGenerator(initialVector, COMBINATIONS_LENGTH);
		return generator.generateAllObjects();
	}

	/**
	 * Converts the given combinatorics vector into an energy data pair
	 * @param vector
	 * @return
	 */
	private EnergyDataPair toValuePair(ICombinatoricsVector<EnergyData> vector) {
		EnergyData firstValue = vector.getValue(0);
		EnergyData secondValue = vector.getValue(1);
		return new EnergyDataPair(firstValue.getTimestamp(),
				new CountryPair(firstValue.getCountry(), secondValue.getCountry()),
				new EnergyValuePair(firstValue.getValue(), secondValue.getValue()));
	}
}
