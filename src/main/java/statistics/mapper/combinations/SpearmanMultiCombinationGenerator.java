package statistics.mapper.combinations;

import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

import org.paukov.combinatorics.Generator;
import org.paukov.combinatorics.ICombinatoricsVector;
import org.paukov.combinatorics.util.ComplexCombinationGenerator;

import lombok.NonNull;
import lombok.Setter;
import schema.CorrelationMeasurePair;
import schema.country.MultiCountryPair;
import schema.entry.DataEntry;
import schema.entry.DataEntryCollection;
import schema.entry.DataEntryPair;
import statistics.Aggregator;

@Setter
public class SpearmanMultiCombinationGenerator extends SpearmanCombinationGenerator{

    private static final int N_COMBINATIONS = 2; // For reducing input and formulating pairs

    @NonNull
    private Aggregator aggregator;

    public SpearmanMultiCombinationGenerator(int combinationLength, Aggregator aggregator) {
        super(combinationLength);
        this.aggregator = aggregator;
    }

    /**
     * Method Overridden for creating an additional partition of combinations
     *
     * Example (p = 4):
     * 								<DE, NL, BR, FR>
     *
     * <DE> <NL, BR, FR>	<NL> <DE, BR, FR>	<BR> <DE, NL, FR>	<FR> <DE, NL, BR>
     * 			<DE, NL> <BR, FR>	<DE, BR> <NL, FR>	<DE, FR> <NL, BR>
     *
     * @param dataEntryVector
     * @return
     */
    @Override
    protected Collection<DataEntryCollection> toDataEntryPairs(ICombinatoricsVector<DataEntry> dataEntryVector) {
        Generator<ICombinatoricsVector<DataEntry>> generator = new ComplexCombinationGenerator<>(dataEntryVector, N_COMBINATIONS);
        List<ICombinatoricsVector<ICombinatoricsVector<DataEntry>>> combinatoricsVectorsList = generator.generateAllObjects();
        return combinatoricsVectorsList.stream().map(this::toDataEntryPair).collect(Collectors.toSet());
    }

    /**
     * Transform the partitioned combinations into a pair of data entries
     * @param combinatoricsVectors
     * @return
     */
    private DataEntryPair toDataEntryPair(ICombinatoricsVector<ICombinatoricsVector<DataEntry>> combinatoricsVectors) {
        List<DataEntry> firstEntries = combinatoricsVectors.getValue(0).getVector();
        List<DataEntry> secondEntries = combinatoricsVectors.getValue(1).getVector();

        MultiCountryPair multiCountryPair = new MultiCountryPair(getCountries(firstEntries), getCountries(secondEntries));
        CorrelationMeasurePair correlationMeasurePair = new CorrelationMeasurePair(aggregateValues(firstEntries), aggregateValues(secondEntries));
        return new DataEntryPair(multiCountryPair, correlationMeasurePair);
    }

    /**
     * Aggregates the values within the provided data entries by the designated method of aggregation
     * @param entries
     * @return
     */
    private Double aggregateValues(List<DataEntry> entries) {
        DoubleStream doubleStream = entries.stream().mapToDouble(DataEntry::getValue);
        return aggregator.getFunction().apply(doubleStream).orElseThrow(() -> new RuntimeException("Missing values"));
    }

    /**
     * Retrieves the list of countries from the data entries
     * @param dataEntries
     * @return
     */
    private Collection<String> getCountries(List<DataEntry> dataEntries) {
        return dataEntries.stream().map(DataEntry::getCountry).collect(Collectors.toSet());
    }
}
