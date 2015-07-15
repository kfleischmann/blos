package eu.blos.java.algorithms.sketches.field_normalizer;

import eu.blos.java.algorithms.sketches.FieldNormalizer;

public class ZeroOneNormalizer implements FieldNormalizer {

	private int pow;

	public ZeroOneNormalizer(int pow ){
		this.pow = pow;
	}

	@Override
	public int normalize(double val) {
		return (int) ( val * Math.pow(2, this.pow ) );
	}

	@Override
	public double denormalize(int val) {
		return (  Math.pow(2, -this.pow )  * (double)val);
	}

	@Override
	public double getMax() {
		return 1;
	}

	@Override
	public double getMin() {
		return 0;
	}
}
