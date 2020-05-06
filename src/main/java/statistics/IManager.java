package statistics;

import java.util.Collection;

import data.DataFile;
import scala.Tuple2;
import schema.CountryPair;

public interface IManager {
	Collection<Tuple2<CountryPair, Double>> correlations(CorrelationType correlationType, DataFile dataFile);
}
