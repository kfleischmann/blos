package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.BloomFilter;
import eu.blos.java.api.common.LearningFunction;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapPartitionFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.util.Collector;
import org.apache.commons.lang.math.IntRange;
import org.jblas.util.Random;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;


public class RFLearning {

	public static int SELECT_FEATURES_PER_NODE = 5;

	public static int NUMBER_NODES = 1;

	public static int NUMBER_TREES = 0;

	public static int NUMBER_TREES_PER_NODE = 0;

	public static boolean fileOutput =  true;

    public static void run(final ExecutionEnvironment env, String sketchDataPath, String outputTreePath, int trees ) throws Exception {

		NUMBER_TREES = trees;

		NUMBER_TREES_PER_NODE = NUMBER_TREES / NUMBER_NODES;

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


		/**
		 * SKETCH STRUCTURE for the learning phase
		 */

		// Knowlege about the sample-labels.
		// Request qj(s, l) -> {0,1}
		private BloomFilter qj = new BloomFilter(2^31-1, 10000 );

		// Knowlege about the feature locations according to the different candidates.
		// Request qjL(s, f, c) -> {0,1}
		private BloomFilter qjL = new BloomFilter(2^31-1, 10000 );

		// Knowlege about the feature locations according to the different candidates.
		// Request qjR(s, f, c) -> {0,1}
		private BloomFilter qjR = new BloomFilter(2^31-1, 10000 );

		private Collector<Tuple1<String>> output;

		public RFLearningOperator(){
			super();
		}

		@Override
		public void mapPartition(Iterator<String> sketch, Collector<Tuple1<String>> output) throws Exception {
			this.output = output;

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
			for( int tree=0; tree < NUMBER_TREES_PER_NODE; tree++ ){
				List<Integer> featureSpace = new ArrayList<Integer>();
				for(int i=0; i < RFSketching.NUMBER_FEATURES; i++ ) featureSpace.add(i);
				int[] baggingTable = createBaggingtable(RFSketching.NUMBER_SAMPLES);
				int[] features = selectRandomFeatures(featureSpace, SELECT_FEATURES_PER_NODE );

				splitNode(baggingTable, features, featureSpace, tree, BigInteger.valueOf(0) );
			}
		}

		/**
		 * this method do all the magic stuff, find the best split, create new nodes and recursively split these
		 * nodes again until a stopping criterion is reached
		 * @param baggingTable
		 * @param treeId
		 * @param nodeId
		 */
		public void splitNode( int baggingTable[], int features[], List<Integer> featureSpace, long treeId, BigInteger nodeId ) {
		}


		/**
		 *
		 * @param sampleCount
		 * @return
		 */
		public int[] createBaggingtable(int sampleCount ){
			int bt[] = new int[sampleCount];
			for( int i=0; i < sampleCount; i++ )
				bt[i] = Random.nextInt(sampleCount);
			return bt;
		}

		/**
		 * select num random features from the feature space
		 * @param featureSpace
		 * @param num
		 * @return
		 */
		public int[] selectRandomFeatures(final List<Integer> featureSpace, int num ){
			List<Integer> tmpFeatureSpace = new ArrayList<Integer>(featureSpace);
			int features[] = new int[num];
			for( int f=0; f < num; f++ ){
				int feature = Random.nextInt(tmpFeatureSpace.size());
				features[f] = tmpFeatureSpace.get(feature);
				tmpFeatureSpace.remove(feature);
			}
			return features;
		}
	}
}
