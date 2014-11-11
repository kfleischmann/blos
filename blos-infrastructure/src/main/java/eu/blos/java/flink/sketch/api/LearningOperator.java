package eu.blos.java.flink.sketch.api;

import eu.blos.java.api.common.LearningFunction;
import org.apache.flink.api.common.functions.MapPartitionFunction;
import org.apache.flink.util.Collector;
import java.io.Serializable;

abstract class LearningOperator<IN,OUT> implements Serializable, MapPartitionFunction<IN, OUT> {

	private LearningFunction<OUT> func;

	public LearningOperator( LearningFunction<OUT> func ){
		super();
		this.func = func;
	}

	@Override
	public void mapPartition(Iterable<IN> sketch, Collector<OUT> output ) throws Exception {
		func.learn(output );
	}
}