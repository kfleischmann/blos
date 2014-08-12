package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.BloomFilter;
import eu.stratosphere.api.java.ExecutionEnvironment;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RFBuilder {
	public static boolean fileOutput =  true;

	private static final Log LOG = LogFactory.getLog(RFBuilder.class);

	public static void main(String[] args ) throws Exception {
		//final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "/home/kay/blos/blos.jar");

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);


		String rawInputPath = "file:///home/kay/datasets/mnist/normalized_small.txt";
		String sketchDataPath=  "file:///home/kay/temp/rf/tree-1-full-mnist/";
		String outputTreePath = "file:///home/kay/temp/rf/tree-1-full-mnist/tree";

		// start Sketching phase
		RFSketching.sketch( env, rawInputPath, sketchDataPath );

		// Start Learning phase
		RFLearning.learn( env, sketchDataPath, outputTreePath, 1 );
	}
}