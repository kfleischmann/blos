package eu.blos.java.stratosphere.sketch.api;

import eu.blos.java.algorithms.sketches.HashFunction;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;

import java.io.Serializable;

public interface SketcherUDF extends Serializable {
	public void sketch(String record, Collector<Tuple3<Long, Integer, Double>> collector, HashFunction[] hashFunctions );
}
