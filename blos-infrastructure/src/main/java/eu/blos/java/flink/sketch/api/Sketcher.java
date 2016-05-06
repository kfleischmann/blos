package eu.blos.java.flink.sketch.api;

import eu.blos.java.sketches.DigestHashFunction;
import eu.blos.java.sketches.HashFunction;
import eu.blos.java.sketches.HashFunctionDescription;

import java.io.Serializable;

public class Sketcher implements Serializable  {

	private String source;
	private String dest;
	private HashFunction[] hashfunctions;
	private HashFunctionDescription hdesc;
	private int sketchType;
	private SketcherUDF udf;
	private int[] groupBy;

	/**
	 *
	 * @param source
	 * @param dest
	 * @param hdesc
	 * @param sketchType
	 */
	public Sketcher(String source, String dest, HashFunctionDescription hdesc, SketcherUDF udf, int sketchType, int[] groupBy ){
		this.source = source;
		this.dest = dest;
		this.hdesc = hdesc;
		this.sketchType = sketchType;
		this.udf = udf;
		this.groupBy = groupBy;
	}

	public String getSource(){ return source; }
	public String getDest(){ return dest; }
	public HashFunctionDescription getHashFunctionDescription(){ return hdesc; }
	public int getSketchType(){ return sketchType; }
	public SketcherUDF getUDF(){return udf;}
	public int[] getGroupBy(){ return groupBy; }
	public HashFunction[] getHashFunctions() {
		return DigestHashFunction.generateHashfunctions( (int)hdesc.d, hdesc.w );
	}
}
