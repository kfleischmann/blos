package eu.blos.java.algorithms.sketches;

import java.math.BigInteger;
import java.util.Arrays;
import java.util.Random;

public class DefaultHashFunction implements HashFunction {
	/**
	 *
	 * @param number number of hash functions
	 * @param w the maximum
	 * @return
	 */
	public static HashFunction[] generateHashfunctions(int number, long w ){
		HashFunction[] h = new HashFunction[number];
		for(int i=0; i < number; i++ ){
			h[i] = new DefaultHashFunction(w);
		}
		return h;
	}

	public static final long PRIME_MODULUS = (1L << 31) - 1;

	public long a = Math.abs(new Random().nextLong());

	public long b = Math.abs(new Random().nextLong());

	public long w = 0;

	public DefaultHashFunction(long w){
		this.w=w;
	}

	public long hash( byte[] x ){
		int hashcode = Arrays.deepHashCode(new Object[]{x});
		return BigInteger.valueOf(a).multiply(BigInteger.valueOf(hashcode)).add( BigInteger.valueOf(b)).mod(BigInteger.valueOf(w)).longValue();
	}

}
