package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.BloomFilter;
import eu.blos.java.api.common.LearningFunction;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapPartitionFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.util.Collector;

import java.io.Serializable;
import java.util.Iterator;


public class RFLearning {

	public static int NUMBER_NODES = 1;

	public static boolean fileOutput =  true;

    public static void main(String[] args) throws Exception {
		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		String rawInputPath = "file:///home/kay/datasets/mnist/normalized_small.txt";
		String sketchDataPath=  "file:///home/kay/temp/rf/tree-1-full-mnist/";


		//RFSketching.main( new String[] { rawInputPath, sketchDataPath } );

		String outputTreePath = "file:///home/kay/temp/rf/tree-1-full-mnist/tree";

		buildtrees( env, sketchDataPath, outputTreePath );
    }

	public static void buildtrees( final ExecutionEnvironment env, String inputPath, String outputPath ) throws Exception {

		DataSet<String> sketch = env.readTextFile(inputPath+"/"+RFSketching.PATH_OUTPUT_SKETCH);

		// do the learning
		DataSet<Tuple1<String>> trees = sketch.mapPartition( new RFLearningOperator() );

		// emit result
		if(fileOutput) {
			trees.writeAsCsv(outputPath, "\n", ",", FileSystem.WriteMode.OVERWRITE);
		} else {
			trees.print();
		}

		// execute program
		env.execute("Sketching phase");
	}


	/**
	 * RandomForest LearningOprator
	 *
	 * this operator reads the sketch into memory built in the previous phase and starts the learning process
	 * output: final trees
	 */
	static class RFLearningOperator extends  MapPartitionFunction<String, Tuple1<String>> implements Serializable, LearningFunction<Tuple1<String>> {

		// Knowlege about the sample-labels.
		// Request qj(s, l) -> {0,1}
		private BloomFilter qj = new BloomFilter(2^31-1, 10000 );

		// Knowlege about the feature locations according to the different candidates.
		// Request qjL(s, f, c) -> {0,1}
		private BloomFilter qjL = new BloomFilter(2^31-1, 10000 );

		// Knowlege about the feature locations according to the different candidates.
		// Request qjR(s, f, c) -> {0,1}
		private BloomFilter qjR = new BloomFilter(2^31-1, 10000 );

		public RFLearningOperator(){
			super();
		}

		@Override
		public void mapPartition(Iterator<String> sketch, Collector<Tuple1<String>> output) throws Exception {
			while(sketch.hasNext()){
				String[] fields = sketch.next().split(" ");

				String sampleId = fields[1];
				String label = fields[2];
				String featureId = fields[3];
				Double featureVal = Double.parseDouble(fields[4]);
				Double splitCandidate = Double.parseDouble(fields[5]);

				qj.add(sampleId + label );

				if( featureVal < splitCandidate ) {
					qjL.add(sampleId + featureId + splitCandidate);
				} else {
					qjR.add(sampleId + featureId + splitCandidate);
				}
			}

			// now start learning phase
			learn(output);
		}


		@Override
		public void learn(Collector<Tuple1<String>> output) {
		}
	}
}
