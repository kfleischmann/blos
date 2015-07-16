package eu.blos.java.algorithms.sketches;

import java.io.Serializable;

public interface FieldNormalizer extends Serializable{
	public int normalize(double val);
	public double denormalize(int val);
	public double getMax();
	public double getMin();
	public int getRandom();
}
