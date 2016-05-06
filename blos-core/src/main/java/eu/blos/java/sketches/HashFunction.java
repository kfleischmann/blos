package eu.blos.java.sketches;

import java.io.Serializable;

public interface HashFunction extends Serializable {
	public long hash( String x );
	public long w();
}
