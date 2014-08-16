package eu.blos.java.algorithms.sketches;

import java.io.Serializable;

public interface HashFunction extends Serializable {
	public long hash( byte[] x );
}
