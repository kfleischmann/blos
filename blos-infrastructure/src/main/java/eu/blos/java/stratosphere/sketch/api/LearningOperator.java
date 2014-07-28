package eu.blos.java.stratosphere.sketch.api;

import eu.blos.java.api.common.LearningFunction;
import eu.stratosphere.api.java.functions.MapPartitionFunction;
import eu.stratosphere.util.Collector;

import java.io.Serializable;
import java.util.Iterator;

abstract class LearningOperator<IN,OUT> extends MapPartitionFunction<IN, OUT> implements Serializable {

	private LearningFunction<OUT> func;

	public LearningOperator( LearningFunction<OUT> func ){
		super();
		this.func = func;
	}

	@Override
	public void mapPartition(Iterator<IN> sketch, Collector<OUT> output ) throws Exception {
		func.learn(output );
	}
}