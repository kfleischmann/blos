package eu.blos.java.flink.sketch.sketch.api;

import eu.blos.java.algorithms.sketches.HashFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public interface SketcherUDF extends Serializable {
	public void sketch(String record, Collector<Tuple4<Long, Integer, Integer, Double>> collector, HashFunction[] hashFunctions );
}
