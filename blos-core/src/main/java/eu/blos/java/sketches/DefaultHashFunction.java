package eu.blos.java.sketches;

import org.apache.commons.lang3.StringUtils;

import java.nio.charset.Charset;

public class DefaultHashFunction implements HashFunction {

	public long w = 0;

	public String seed;

	public DefaultHashFunction(long w, long seed )  {
		this.w=w;
		this.seed=StringUtils.leftPad("" + seed, 5, "0");
	}
	// encoding used for storing hash values as strings
	private static final Charset charset = Charset.forName("UTF-8");

	@Override
	public long hash(String text) {
		String  key = ""+seed+text;
		return -1;
	}

	@Override
	public long w() {
		return 0;
	}
}
