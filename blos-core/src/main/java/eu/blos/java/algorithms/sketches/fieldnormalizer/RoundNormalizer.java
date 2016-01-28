package eu.blos.java.algorithms.sketches.fieldnormalizer;

import eu.blos.java.algorithms.sketches.InputSpaceNormalizer;

public class RoundNormalizer implements InputSpaceNormalizer<Double> {

	private static Double round (double value, int precision) {
		int scale = (int) Math.pow(10, precision);
		return (double)Math.round(value * scale) / scale;
	}
	private int precision;
	private long total_elements;

	public RoundNormalizer(int precision){
		this.precision = precision;
		// 2 times because of -1 to 1
		this.total_elements = 2*(long)Math.pow(10, precision);
	}

	@Override
	public Double normalize(double val) {
		return round(val, precision);
	}

	@Override
	public double denormalize(Double val) {
		return val;
	}

	@Override
	public Double getMax() {
		return 1.0;
	}

	@Override
	public Double getMin() {
		return -1.0;
	}

	@Override
	public Double getStep() {
		return Math.pow(10, -precision);
	}

	@Override
	public Double getRandom() {
		return new java.util.Random().nextFloat()*2.0 - 1.0;
	}

	@Override
	public long getTotalElements() {
		return this.total_elements;
	}
}
