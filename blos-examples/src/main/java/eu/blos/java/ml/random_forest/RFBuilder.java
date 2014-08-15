package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.BloomFilter;
import eu.blos.java.algorithms.sketches.HashFunction;
import eu.blos.java.stratosphere.sketch.api.SketchBuilder;
import eu.blos.java.stratosphere.sketch.api.SketcherUDF;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class RFBuilder {
	public static boolean fileOutput =  true;

	private static final Log LOG = LogFactory.getLog(RFBuilder.class);

	public static void main(String[] args ) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "/home/kay/blos/blos.jar");

		//final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);


		String rawInputPath = "file:///home/kay/datasets/mnist/normalized_small.txt";
		String preprocessedDataPath=  "file:///home/kay/temp/rf/tree-1-full-mnist/preprocessed";
		String sketchDataPath=  "file:///home/kay/temp/rf/tree-1-full-mnist/sketched";
		String outputTreePath = "file:///home/kay/temp/rf/tree-1-full-mnist/tree";

		// ------------------------------------------
		// start preprocessing phase
		// ------------------------------------------

		//RFPreprocessing.process(env, rawInputPath, preprocessedDataPath);

		BloomFilter bfNode 				= new BloomFilter(0.3, 10000);
		BloomFilter bfSplitCandidate 	= new BloomFilter(0.3, 10000);
		BloomFilter bfSampleSketch 		= new BloomFilter(0.3, 10000);


		// ------------------------------------------
		// start sketching phase
		// ------------------------------------------



		SketchBuilder.sketch(	env,
								preprocessedDataPath, sketchDataPath,
								SketchBuilder.apply( RFPreprocessing.PATH_OUTPUT_SKETCH_NODE,
													 RFPreprocessing.PATH_OUTPUT_SKETCH_NODE+"-left",
														bfNode.getHashFunctions(), SketchBuilder.SKETCHTYPE_BLOOM_FILTER, new SketcherUDF() {
									private SketchBuilder.DefaultSketcherUDF defaultSketcher = new SketchBuilder.DefaultSketcherUDF();

									@Override
									public void sketch(String record, Collector<Tuple3<Long, Integer, Integer>> collector, HashFunction[] hashFunctions) {
										// only sketch left
										String[] values = record.split(",");
										Double featureValue = Double.parseDouble(values[3]);
										Double splitCandidate = Double.parseDouble(values[4]);
										if(featureValue<=splitCandidate){

											defaultSketcher.sketch(record, collector, hashFunctions );
										}
									}
								}),
								SketchBuilder.apply( 	RFPreprocessing.PATH_OUTPUT_SKETCH_NODE,
														RFPreprocessing.PATH_OUTPUT_SKETCH_NODE+"-right",
														bfNode.getHashFunctions(), SketchBuilder.SKETCHTYPE_BLOOM_FILTER, new SketcherUDF() {
									private SketchBuilder.DefaultSketcherUDF defaultSketcher = new SketchBuilder.DefaultSketcherUDF();
									@Override
									public void sketch(String record, Collector<Tuple3<Long, Integer, Integer>> collector, HashFunction[] hashFunctions) {

										// only sketch right
										String[] values = record.split(",");
										Double featureValue = Double.parseDouble(values[3]);
										Double splitCandidate = Double.parseDouble(values[4]);

										if(featureValue>splitCandidate){
											defaultSketcher.sketch(record, collector, hashFunctions );
										}
									}

								}),
//								SketchBuilder.apply( RFPreprocessing.PATH_OUTPUT_SKETCH_SPLIT_CANDIDATES, bfSplitCandidate.getHashFunctions(), SketchBuilder.SKETCHTYPE_BLOOM_FILTER ),
								SketchBuilder.apply( RFPreprocessing.PATH_OUTPUT_SKETCH_SAMPLE_LABELS, RFPreprocessing.PATH_OUTPUT_SKETCH_SAMPLE_LABELS, bfSampleSketch.getHashFunctions(), SketchBuilder.SKETCHTYPE_BLOOM_FILTER )
							);


		// ------------------------------------------
		// Start Learning phase
		// ------------------------------------------

		//RFLearning.learn(env, sketchDataPath, outputTreePath, 1);
	}
}