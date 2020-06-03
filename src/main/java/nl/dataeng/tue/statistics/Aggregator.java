package nl.dataeng.tue.statistics;

import java.util.OptionalDouble;
import java.util.stream.DoubleStream;

import com.google.common.base.Function;

public enum Aggregator {
	MAX	(DoubleStream::max),
	MIN	(DoubleStream::min),
	AVG	(DoubleStream::average);
//	IDENTITY (Function::identity); What would the point of this be?

	private final Function<DoubleStream, OptionalDouble> function;

	Aggregator(Function<DoubleStream, OptionalDouble> function) {
		this.function = function;
	}

	public Function<DoubleStream, OptionalDouble> getFunction() {
		return function;
	}
}
