package eu.blos.java.algorithms.sketches;

import java.io.Serializable;
import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class DigestHashFunction implements HashFunction {

	/**
	 * this method generate a list od hashfunction
	 *
	 * @param number number of hash functions
	 * @param w      the maximum
	 * @return
	 */
	public static HashFunction[] generateHashfunctions(int number, long w) {
		HashFunction[] h = new HashFunction[number];
		for (byte i = 0; i < number; i++) {
			h[i] = new DigestHashFunction(w, i);
		}
		return h;
	}

	// encoding used for storing hash values as strings
	private static final Charset charset = Charset.forName("UTF-8");

	private static MessageDigest digest;

	private long w;

	private long seed;


	static {
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}

	public DigestHashFunction(long w, long seed) {
		this.w = w;
		this.seed = seed;
	}

	@Override
	public long hash(String text) {
		return hash((text + seed).getBytes(charset));
	}

	@Override
	public long hash(byte[] x) {
		byte[] hash = digest.digest(x);
		long value = 0;
		for (int i = 0; i < hash.length; i++) {
			value += ((long) hash[i] & 0xffL) << (8 * i);
		}
		return Math.abs(value % w);
	}
}