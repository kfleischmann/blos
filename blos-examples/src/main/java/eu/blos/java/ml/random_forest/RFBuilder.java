package eu.blos.java.ml.random_forest;

import eu.stratosphere.api.java.ExecutionEnvironment;

public class RFBuilder {
	public static void main(String[] args ) throws Exception {
		//final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "/home/kay/blos/blos.jar");


		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		String rawInputPath = "file:///home/kay/datasets/mnist/normalized_small.txt";
		String sketchDataPath=  "file:///home/kay/temp/rf/tree-1-full-mnist/";

		String outputTreePath = "file:///home/kay/temp/rf/tree-1-full-mnist/tree";


		//RFSketching.run( env, rawInputPath, sketchDataPath );

		RFLearning.run( env, sketchDataPath, outputTreePath, 1 );
	}
}