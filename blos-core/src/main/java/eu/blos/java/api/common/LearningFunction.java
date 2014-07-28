package eu.blos.java.api.common;

import eu.stratosphere.util.Collector;

public interface LearningFunction<OUT> {
	public void learn(Collector<OUT> output);
}
