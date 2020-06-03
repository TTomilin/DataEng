package nl.dataeng.tue.statistics.manager;

import java.util.Collection;

import nl.dataeng.tue.data.DataFile;
import scala.Tuple2;
import nl.dataeng.tue.schema.country.CountryCollection;

public interface ICorrelationManager {
	Collection<Tuple2<CountryCollection, Double>> calculateCorrelations(DataFile dataFile);
}
