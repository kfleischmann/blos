package eu.blos.java.sketches;


import java.nio.charset.Charset;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import org.apache.commons.lang3.StringUtils;


public class DigestHashFunction implements HashFunction {

	/**
	 * this method generates a list of hashfunctions
	 *
	 * @param number number of hash functions
	 * @param w      the maximum
	 * @return
	 */
	public static HashFunction[] generateHashfunctions(int number, long w) {
		HashFunction[] h = new HashFunction[number];
		for (byte i = 0; i < number; i++) {
			h[i] = new DigestHashFunction(w, i);
		}//for
		return h;
	}

	// encoding used for storing hash values as strings
	private static final Charset charset = Charset.forName("UTF-8");

	private MessageDigest digest;

	private long w;

	private String seed;

	/*static {
		try {

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}*/

	public DigestHashFunction(long w, long seed) {
		try {
			digest = MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		this.w = w;
		this.seed = StringUtils.leftPad( ""+seed, 5, "0" );
	}

	@Override
	public long hash(String text) {
		return hash((""+seed+text ).getBytes(charset));
	}

	@Override
	public long w() {
		return w;
	}

	private long hash(byte[] x) {
		byte[] hash = digest.digest(x);
		long value=0;
		for ( int b=0; b < 8; b++ ){
			value ^= ((long)hash[b]) << (8*b);
		}//for

		long result = Math.abs( (long) (((double)value/(double)Long.MAX_VALUE ) *w) );
		return result;
	}
}