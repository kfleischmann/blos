package eu.blos.java.algorithms.sketches.field_normalizer;

import eu.blos.java.algorithms.sketches.FieldNormalizer;

import java.util.Random;

public class RoundNormalizer implements FieldNormalizer<Double> {

	private static Double round (double value, int precision) {
		int scale = (int) Math.pow(10, precision);
		return (double)Math.round(value * scale) / scale;
	}
	private int precision;

	public RoundNormalizer(int precision){
		this.precision = precision;
	}

	@Override
	public Double normalize(double val) {
		return round(val, precision);
	}

	@Override
	public double denormalize(Double val) {
		return 0;
	}

	@Override
	public Double getMax() {
		return null;
	}

	@Override
	public Double getMin() {
		return null;
	}

	@Override
	public Double getRandom() {
		return null;
	}
}
