package statistics.manager;

import java.util.Collection;

import data.DataFile;
import scala.Tuple2;
import schema.country.CountryCollection;

public interface ICorrelationManager {
	Collection<Tuple2<CountryCollection, Double>> calculateCorrelations(DataFile dataFile);
}
