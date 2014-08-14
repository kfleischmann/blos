package eu.blos.java.api.common;


import eu.blos.java.algorithms.sketches.HashFunction;
import eu.blos.java.algorithms.sketches.Sketch;

import java.util.List;

public class SketchBuilder {

	public static Sketcher map( String key, List<HashFunction> hashfunctions, int type ){
		return new Sketcher( key, hashfunctions, type );
	}

	public static void sketch( String preprocessedDataPath, String sketchDataPath, Sketcher... mapper ){

	}
}
