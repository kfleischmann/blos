package eu.blos.java.ml.linear_regression;

import eu.blos.java.algorithms.sketches.Sketch;
import eu.stratosphere.api.java.ExecutionEnvironment;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class Learner {
	private static final Log LOG = LogFactory.getLog(Learner.class);

	/**
	 * Run the Learning phase
	 *
	 * @param env
	 * @param sketchDataPath
	 * @param outputTreePath
	 * @param sketches
	 * @param args
	 * @throws Exception
	 */
	public static void learn(final ExecutionEnvironment env,
							 String preprocessedDataPath,
							 String sketchDataPath,
							 String outputTreePath,
							 Sketch[] sketches, String ... args ) throws Exception {
	}
}
