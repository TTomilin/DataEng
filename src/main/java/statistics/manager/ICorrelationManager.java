package statistics.manager;

import java.util.Collection;

import data.DataFile;
import scala.Tuple2;
import schema.MultiCountryPair;

public interface ICorrelationManager {
	Collection<Tuple2<MultiCountryPair, Double>> calculateCorrelations(DataFile dataFile);
}
