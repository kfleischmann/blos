package eu.blos.java.algorithms.sketches;

import java.io.Serializable;

public interface FieldNormalizer<T> extends Serializable{
	public T normalize(double val);
	public double denormalize(T val);
	public T getMax();
	public T getMin();
	public T getRandom();
}
