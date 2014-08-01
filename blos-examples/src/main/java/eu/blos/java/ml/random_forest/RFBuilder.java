package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.BloomFilter;
import eu.stratosphere.api.java.ExecutionEnvironment;

public class RFBuilder {
	public static void main(String[] args ) throws Exception {
		//final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "/home/kay/blos/blos.jar");



		//System.out.println(RFSketching.NUMBER_SAMPLES*RFSketching.NUMBER_LABELS *RFSketching.maxSplitCandidates);
		//System.out.println(RFSketching.NUMBER_SAMPLES*RFSketching.NUMBER_FEATURES *RFSketching.maxSplitCandidates);

		//System.out.println( (int)(Math.pow(2,31)-1) );

		/*
		BloomFilter sketch_qj = new BloomFilter( 0.5, RFSketching.NUMBER_SAMPLES*RFSketching.NUMBER_FEATURES *RFSketching.maxSplitCandidates );

		System.out.println( sketch_qj.getK() );
		System.out.println( sketch_qj.getBitSet().size() );

		//sketch_qj.add("test");
		System.out.println(sketch_qj.contains("test".getBytes()) );*/


		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);

		String rawInputPath = "file:///home/kay/datasets/mnist/normalized_small.txt";
		String sketchDataPath=  "file:///home/kay/temp/rf/tree-1-full-mnist/";

		String outputTreePath = "file:///home/kay/temp/rf/tree-1-full-mnist/tree";


		//RFSketching.run( env, rawInputPath, sketchDataPath );

		RFLearning.run( env, sketchDataPath, outputTreePath, 1 );

	}
}