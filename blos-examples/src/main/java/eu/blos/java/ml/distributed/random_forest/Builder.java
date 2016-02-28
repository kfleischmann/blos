package eu.blos.java.ml.distributed.random_forest;

import eu.blos.java.sketches.*;
import eu.blos.java.flink.sketch.api.SketchBuilder;
import eu.blos.java.flink.sketch.api.SketcherUDF;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.util.Collector;

public class Builder {
	public static boolean fileOutput =  true;

	private static final Log LOG = LogFactory.getLog(Builder.class);

	public static void main(String[] args ) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.createRemoteEnvironment("localhost", 6123, "/home/kay/blos/blos.jar");
		//final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);

		final BloomFilter bfNodeLeft 	 = new BloomFilter(0.5, (int)(Preprocessor.NUM_SAMPLES* Preprocessor.NUM_SAMPLE_FEATURES * Preprocessor.HISTOGRAM_SPLIT_CANDIDATES*0.5) );
		final BloomFilter bfNodeRight 	 = new BloomFilter(0.5, (int)(Preprocessor.NUM_SAMPLES* Preprocessor.NUM_SAMPLE_FEATURES * Preprocessor.HISTOGRAM_SPLIT_CANDIDATES*0.5) );
		final BloomFilter bfSampleSketch = new BloomFilter(0.5, Preprocessor.NUM_SAMPLES );


		String rawInputPath	= 			"file:///home/kay/datasets/mnist/normalized_full.txt";
		String preprocessedDataPath=  	"file:///home/kay/temp/rf/tree-1-test-mnist-small-05/preprocessed";
		String sketchDataPath=  		"file:///home/kay/temp/rf/tree-1-test-mnist-small-05/sketched";
		String outputTreePath = 		"file:///home/kay/temp/rf/tree-1-test-mnist-small-05/tree/tree";

		// ------------------------------------------
		// start preprocessing phase
		// the preprocessor prepares the raw data for the sketchin phase
		// ------------------------------------------
		Preprocessor.transform(env, rawInputPath, preprocessedDataPath);


		// ------------------------------------------
		// start sketching phase
		// ------------------------------------------
		SketchBuilder.sketch(
			env, preprocessedDataPath, sketchDataPath,
				SketchBuilder.apply(
					Preprocessor.PATH_OUTPUT_SKETCH_NODE,
					Preprocessor.PATH_OUTPUT_SKETCH_NODE+"-left",
					bfNodeLeft.getHashfunctions(),SketchBuilder.SKETCHTYPE_BLOOM_FILTER,
						new SketcherUDF() {
							private SketchBuilder.FieldSketcherUDF defaultSketcher =
									new SketchBuilder.FieldSketcherUDF(",", SketchBuilder.Fields(0, 2, 4) );

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
					Preprocessor.PATH_OUTPUT_SKETCH_NODE,
					Preprocessor.PATH_OUTPUT_SKETCH_NODE+"-right",
					bfNodeRight.getHashfunctions(), SketchBuilder.SKETCHTYPE_BLOOM_FILTER,
						new SketcherUDF() {
							private SketchBuilder.FieldSketcherUDF defaultSketcher =
									new SketchBuilder.FieldSketcherUDF(",", SketchBuilder.Fields(0, 2, 4));
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

		Learner.learn(env, preprocessedDataPath, sketchDataPath, outputTreePath, sketches, "1");

	}
}