package eu.blos.java.stratosphere.sketch.api;

import eu.blos.java.algorithms.sketches.HashFunction;
import eu.stratosphere.api.java.tuple.Tuple4;
import eu.stratosphere.util.Collector;

import java.io.Serializable;

public interface SketcherUDF extends Serializable {
	public void sketch(String record, Collector<Tuple4<Long, Integer, Integer, Double>> collector, HashFunction[] hashFunctions );
}
