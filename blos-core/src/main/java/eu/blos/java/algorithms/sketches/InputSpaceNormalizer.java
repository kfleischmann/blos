package eu.blos.java.algorithms.sketches;

import java.io.Serializable;

public interface InputSpaceNormalizer<T> extends Serializable{
	public T normalize(double val);
	public double denormalize(T val);
	public T getMax();
	public T getMin();
	public T getStep();
	public T getRandom();
}
