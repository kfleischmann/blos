package eu.blos.java.flink.sketch.api;

import eu.blos.java.algorithms.sketches.HashFunction;

import java.io.Serializable;

public class Sketcher implements Serializable  {

	private String source;
	private String dest;
	private HashFunction[] hashfunctions;
	private int sketchType;
	private SketcherUDF udf;
	private int[] groupBy;

	/**
	 *
	 * @param source
	 * @param dest
	 * @param hashfunctions
	 * @param sketchType
	 */
	public Sketcher(String source, String dest, HashFunction[] hashfunctions, SketcherUDF udf, int sketchType, int[] groupBy ){
		this.source = source;
		this.dest = dest;
		this.hashfunctions = hashfunctions;
		this.sketchType = sketchType;
		this.udf = udf;
		this.groupBy = groupBy;
	}

	public String getSource(){ return source; }
	public String getDest(){ return dest; }
	public HashFunction[] getHashFunctions(){ return this.hashfunctions; }
	public int getSketchType(){ return sketchType; }
	public SketcherUDF getUDF(){return udf;}
	public int[] getGroupBy(){ return groupBy; }
}
