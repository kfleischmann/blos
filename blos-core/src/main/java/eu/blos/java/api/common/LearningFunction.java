package eu.blos.java.api.common;

import org.apache.flink.util.Collector;

public interface LearningFunction<OUT> {
	public void learn(Collector<OUT> output);
}
