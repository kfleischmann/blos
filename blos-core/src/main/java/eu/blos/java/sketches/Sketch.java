package eu.blos.java.sketches;


import java.io.Serializable;

public interface Sketch extends Serializable  {
	public HashFunction[] getHashfunctions();
}