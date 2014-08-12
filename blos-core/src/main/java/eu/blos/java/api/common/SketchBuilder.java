package eu.blos.java.api.common;


import eu.blos.java.algorithms.sketches.Sketch;

public class SketchBuilder {
	public static SketchMapper map( String key, Sketch sketch ){
		return new SketchMapper( key, sketch );
	}
}
