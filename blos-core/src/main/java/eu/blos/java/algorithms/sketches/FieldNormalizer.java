package eu.blos.java.algorithms.sketches;

import java.io.Serializable;

public interface FieldNormalizer extends Serializable{
	public int normalize(double val);
	public double denormalize(int val);
	public int getMax();
	public int getMin();
	public int getRandom();
}
