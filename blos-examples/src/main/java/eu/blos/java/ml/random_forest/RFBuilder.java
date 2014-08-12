package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.BloomFilter;
import eu.blos.java.api.common.SketchBuilder;
import eu.blos.java.api.common.SketchMapper;
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
		String preprocessedDataPath=  "file:///home/kay/temp/rf/tree-1-full-mnist/preprocessed";
		String sketchDataPath=  "file:///home/kay/temp/rf/tree-1-full-mnist/sketched";
		String outputTreePath = "file:///home/kay/temp/rf/tree-1-full-mnist/tree";

		// start preprocessing phase
		RFPreprocessing.process(env, rawInputPath, preprocessedDataPath);

		// start sketching phase
		RFSketching.sketch(	preprocessedDataPath, sketchDataPath,
							SketchBuilder.map("node-sketch", 		new BloomFilter(0.3, 10000)),
							SketchBuilder.map("split-candidate", 	new BloomFilter(0.3, 10000)),
							SketchBuilder.map("sample-sketch", 		new BloomFilter(0.3, 10000))
							);

		// Start Learning phase
		RFLearning.learn(env, sketchDataPath, outputTreePath, 1);
	}
}