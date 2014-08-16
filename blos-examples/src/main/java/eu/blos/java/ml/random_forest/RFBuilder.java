package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.BloomFilter;
import eu.blos.java.algorithms.sketches.HashFunction;
import eu.blos.java.algorithms.sketches.Sketch;
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
		//final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "/home/kay/blos/blos.jar");

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);


		String rawInputPath = "file:///home/kay/datasets/mnist/normalized_small.txt";
		String preprocessedDataPath=  "file:///home/kay/temp/rf/tree-1-full-mnist/preprocessed";
		String sketchDataPath=  "file:///home/kay/temp/rf/tree-1-full-mnist/sketched";
		String outputTreePath = "file:///home/kay/temp/rf/tree-1-full-mnist/tree";

		// ------------------------------------------
		// start preprocessing phase
		// ------------------------------------------

		//RFPreprocessing.process(env, rawInputPath, preprocessedDataPath);



		// ------------------------------------------
		// start sketching phase
		// ------------------------------------------

		BloomFilter bfNodeLeft 			= new BloomFilter(0.3, RFPreprocessing.NUM_SAMPLES* RFPreprocessing.NUM_SAMPLE_FEATURES * RFPreprocessing.HISTOGRAM_SPLIT_CANDIDATES );
		BloomFilter bfNodeRight 		= new BloomFilter(0.3, RFPreprocessing.NUM_SAMPLES* RFPreprocessing.NUM_SAMPLE_FEATURES * RFPreprocessing.HISTOGRAM_SPLIT_CANDIDATES );
		BloomFilter bfSampleSketch 		= new BloomFilter(0.3, RFPreprocessing.NUM_SAMPLES );


		SketchBuilder.sketch(	env,
								preprocessedDataPath, sketchDataPath,
								SketchBuilder.apply( 	RFPreprocessing.PATH_OUTPUT_SKETCH_NODE,
													 	RFPreprocessing.PATH_OUTPUT_SKETCH_NODE+"-left",
														bfNodeLeft.getHashFunctions(), SketchBuilder.SKETCHTYPE_BLOOM_FILTER,
															new SketcherUDF() {
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
														},
														SketchBuilder.ReduceSketchByFields(0)),
								SketchBuilder.apply( 	RFPreprocessing.PATH_OUTPUT_SKETCH_NODE,
														RFPreprocessing.PATH_OUTPUT_SKETCH_NODE+"-right",
														bfNodeRight.getHashFunctions(), SketchBuilder.SKETCHTYPE_BLOOM_FILTER,
															new SketcherUDF() {
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
															},
															SketchBuilder.ReduceSketchByFields(0)),
								SketchBuilder.apply( 	RFPreprocessing.PATH_OUTPUT_SKETCH_SAMPLE_LABELS,
														RFPreprocessing.PATH_OUTPUT_SKETCH_SAMPLE_LABELS,
														bfSampleSketch.getHashFunctions(),
														SketchBuilder.SKETCHTYPE_BLOOM_FILTER,
														SketchBuilder.ReduceSketchByFields(0)
													)
							);

		// ------------------------------------------
		// Start Learning phase
		// ------------------------------------------
		Sketch[] sketches = {bfSampleSketch, bfNodeLeft, bfNodeRight};


		RFLearning.learn(env, preprocessedDataPath, sketchDataPath, outputTreePath, sketches, "1");
	}
}