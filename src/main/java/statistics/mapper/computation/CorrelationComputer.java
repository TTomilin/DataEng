package statistics.mapper.computation;

import org.apache.spark.api.java.function.Function;

public abstract class CorrelationComputer<T> implements Function<T, Double> {
}
