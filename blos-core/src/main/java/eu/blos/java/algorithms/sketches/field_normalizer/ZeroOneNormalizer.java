package eu.blos.java.algorithms.sketches.field_normalizer;

import eu.blos.java.algorithms.sketches.FieldNormalizer;

import java.util.Random;

public class ZeroOneNormalizer implements FieldNormalizer<Integer> {

	private int pow;
	private Random r = new java.util.Random();
	public ZeroOneNormalizer(int pow ){
		this.pow = pow;
	}

	@Override
	public Integer normalize(double val) {
		return (int) ( val * Math.pow(2, this.pow ) );
	}

	@Override
	public double denormalize(Integer val) {
		return (  Math.pow(2, -this.pow )  * (double)val);
	}

	@Override
	public Integer getMax() {
		return (int)Math.pow(2, pow);
	}

	@Override
	public Integer getMin() {
		return - (int)Math.pow(2, pow);
	}

	@Override
	public Integer getRandom() {
		return r.nextInt( 2*(int)Math.pow(2, pow) ) - (int)Math.pow(2, pow) ;
	}
}

