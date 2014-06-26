package eu.blos.java.algorithms.sketches;

import java.math.BigInteger;
import java.util.Random;

public class HashFunction {

	public static HashFunction[] generateHashfunctions(int number, long w ){
		HashFunction[] h = new HashFunction[number];
		for(int i=0; i < number; i++ ){
			h[i] = new HashFunction(w);
		}
		return h;
	}

	public static final long PRIME_MODULUS = (1L << 31) - 1;

	private long a = Math.abs(new Random().nextLong());

	private long b = Math.abs(new Random().nextLong());

	private long w = 0;

	public HashFunction(long w){
		this.w=w;
	}

	public long hash( Long x ){
		return BigInteger.valueOf(a).multiply(BigInteger.valueOf(x)).add( BigInteger.valueOf(b)).mod(BigInteger.valueOf(w)).longValue();
	}

}
