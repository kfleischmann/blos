package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.*;
import eu.blos.java.stratosphere.sketch.api.SketchBuilder;
import eu.blos.java.stratosphere.sketch.api.SketcherUDF;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

import java.security.MessageDigest;

public class RFBuilder {
	public static boolean fileOutput =  true;

	private static final Log LOG = LogFactory.getLog(RFBuilder.class);

	public static void main(String[] args ) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "/home/kay/blos/blos.jar");
		//final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setDegreeOfParallelism(1);


		final BloomFilter bfNodeLeft 	 = new BloomFilter(0.5, (int)(RFPreprocessing.NUM_SAMPLES* RFPreprocessing.NUM_SAMPLE_FEATURES * RFPreprocessing.HISTOGRAM_SPLIT_CANDIDATES*0.5) );
		final BloomFilter bfNodeRight 	 = new BloomFilter(0.5, (int)(RFPreprocessing.NUM_SAMPLES* RFPreprocessing.NUM_SAMPLE_FEATURES * RFPreprocessing.HISTOGRAM_SPLIT_CANDIDATES*0.5) );
		final BloomFilter bfSampleSketch = new BloomFilter(0.5, RFPreprocessing.NUM_SAMPLES );


		String rawInputPath	= 			"file:///home/kay/datasets/mnist/normalized_full.txt";
		String preprocessedDataPath=  	"file:///home/kay/temp/rf/tree-1-test-mnist-small-05/preprocessed";
		String sketchDataPath=  		"file:///home/kay/temp/rf/tree-1-test-mnist-small-05/sketched";
		String outputTreePath = 		"file:///home/kay/temp/rf/tree-1-test-mnist-small-05/tree/tree";

		// ------------------------------------------
		// start preprocessing phase
		// the preprocessor prepares the raw data for the sketchin phase
		// ------------------------------------------
		RFPreprocessing.transform(	env, rawInputPath, preprocessedDataPath);


		// ------------------------------------------
		// start sketching phase
		// ------------------------------------------
		SketchBuilder.sketch(	env,
								preprocessedDataPath, sketchDataPath,

								SketchBuilder.apply( 	RFPreprocessing.PATH_OUTPUT_SKETCH_NODE,
													 	RFPreprocessing.PATH_OUTPUT_SKETCH_NODE+"-left",
														bfNodeLeft.getHashFunctions(),SketchBuilder.SKETCHTYPE_BLOOM_FILTER,
															new SketcherUDF() {
																private SketchBuilder.DefaultSketcherUDF defaultSketcher =
																		new SketchBuilder.DefaultSketcherUDF(",", SketchBuilder.Fields(0, 2, 4) );

																@Override
																public void sketch(String record, Collector<Tuple4<Long, Integer, Integer,Double>> collector, HashFunction[] hashFunctions ) {
																	// only sketch left
																	String[] values = record.split(",");
																	Double featureValue = Double.parseDouble(values[3]);
																	Double splitCandidate = Double.parseDouble(values[4]);

																	if(featureValue<=splitCandidate){
																		defaultSketcher.sketch(record, collector, hashFunctions );

																	} else {
																	}
																}
															},
														SketchBuilder.ReduceSketchByFields(0)),
													SketchBuilder.apply(
														RFPreprocessing.PATH_OUTPUT_SKETCH_NODE,
														RFPreprocessing.PATH_OUTPUT_SKETCH_NODE+"-right",
														bfNodeRight.getHashFunctions(), SketchBuilder.SKETCHTYPE_BLOOM_FILTER,
															new SketcherUDF() {
																private SketchBuilder.DefaultSketcherUDF defaultSketcher =
																		new SketchBuilder.DefaultSketcherUDF(",", SketchBuilder.Fields(0, 2, 4));
																@Override
																public void sketch(String record, Collector<Tuple4<Long, Integer, Integer, Double>> collector, HashFunction[] hashFunctions ) {
																	// only sketch right
																	String[] values = record.split(",");
																	Double featureValue = Double.parseDouble(values[3]);
																	Double splitCandidate = Double.parseDouble(values[4]);

																	if(featureValue>splitCandidate){
																		defaultSketcher.sketch(record, collector, hashFunctions );
																	}else {
																	}
																}
															},
															SketchBuilder.ReduceSketchByFields(0))

								);

		// ------------------------------------------
		// Start Learning phase
		// ------------------------------------------
		Sketch[] sketches = {bfNodeLeft, bfNodeRight, bfSampleSketch};

		RFLearning.learn(env, preprocessedDataPath, sketchDataPath, outputTreePath, sketches, "1");

	}
}