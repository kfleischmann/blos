package eu.blos.java.sketches;

import java.math.BigInteger;
import java.util.Arrays;

public class DefaultHashFunction implements HashFunction {

	public static final long PRIME_MODULUS = (1L << 31) - 1;

	public long a = 378553; //Math.abs(new Random().nextLong()%10000);

	public long b = 63689; //Math.abs(new Random().nextLong()&10000);

	public long w = 0;

	public DefaultHashFunction(long w)  {
		this.w=w;
	}

	@Override
	public long hash(String x) {
		return 0;
	}

	public long hash( byte[] x ){
		int hashcode = Arrays.deepHashCode(new Object[]{x});
		return BigInteger.valueOf(a).multiply(BigInteger.valueOf(hashcode)).add( BigInteger.valueOf(b)).mod(BigInteger.valueOf(w)).longValue();
	}

	public long[] hash(byte[] x, int count ){return null;}

}
