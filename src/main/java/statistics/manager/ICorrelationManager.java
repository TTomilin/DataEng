package statistics.manager;

import java.util.Collection;

import data.DataFile;
import scala.Tuple2;
import schema.CountryPair;

public interface ICorrelationManager {
	Collection<Tuple2<CountryPair, Double>> calculateCorrelations(DataFile dataFile);
}
