package eu.blos.java.flink.sketch.api;

import eu.blos.java.sketches.HashFunction;
import eu.blos.java.sketches.HashFunctionDescription;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.io.Serializable;

public interface SketcherUDF extends Serializable {
	public void sketch(String record, Collector<Tuple4<Long, Integer, Integer, Double>> collector, HashFunctionDescription hdesc);
}
