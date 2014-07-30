package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.BloomFilter;
import eu.blos.java.api.common.LearningFunction;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapPartitionFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.util.Collector;
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

	public static int minNrOfSplitItems = 50;

	/**
	 * Run the Learning phase
	 *
	 * @param env
	 * @param sketchDataPath
	 * @param outputTreePath
	 * @param trees
	 * @throws Exception
	 */
    public static void run(final ExecutionEnvironment env, String sketchDataPath, String outputTreePath, int trees ) throws Exception {

		// remember the number of tress
		NUMBER_TREES = trees;

		// how many trees per node?
		NUMBER_TREES_PER_NODE = NUMBER_TREES / NUMBER_NODES;

		// prepare environment, distribute the sketches to all nodes and start learning phase
		readSketchesAndLearn(env, new String[]{
						sketchDataPath + "/" + RFSketching.PATH_OUTPUT_SKETCH_NODE,
						sketchDataPath + "/" + RFSketching.PATH_OUTPUT_SKETCH_SPLIT_CANDIDATES,
						sketchDataPath + "/" + RFSketching.PATH_OUTPUT_SKETCH_BAGGINGTABLE },
				outputTreePath);
    }

	/**
	 *
	 * @param env
	 * @param sketch_sources
	 * @param outputPath
	 * @throws Exception
	 */
	public static void readSketchesAndLearn( final ExecutionEnvironment env, String[] sketch_sources, String outputPath ) throws Exception {

		// read sketches into memory
		DataSet<String> sketches = null;

		for( String source : sketch_sources ) {
			if(sketches == null ){
				sketches = env.readTextFile( source );
			} else {
				// read new source
				DataSet<String> sketch_source = env.readTextFile( source );

				// append reading
				sketches = sketches.union( sketch_source  );
			}
		}

		// do the learning
		DataSet<Tuple1<String>> trees = sketches.mapPartition( new RFLearningOperator() );

		// emit result
		if(fileOutput) {
			trees.writeAsCsv(outputPath, "\n", ",", FileSystem.WriteMode.OVERWRITE);
		} else {
			trees.print();
		}

		// execute program
		env.execute("Learning phase");
	}


	/**
	 * RandomForest LearningOprator
	 *
	 * this operator reads the sketch into memory built in the previous phase and starts the learning process
	 * output: final trees
	 */
	static class RFLearningOperator extends  MapPartitionFunction<String, Tuple1<String>> implements Serializable, LearningFunction<Tuple1<String>> {


		// --------------------------------------------------
		// SKETCH STRUCTURE for the learning phase
		// --------------------------------------------------


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

		// Knowlege about all split-canidates computed in the sketching phase
		private Map<Integer, String[]> splitCandidates = new HashMap<Integer, String[]>();

		//private Map<String, String> sampleLabels = new HashMap<String, String>();

		private List<Tuple2<Integer,Integer>> baggingTable = new ArrayList<Tuple2<Integer,Integer>>();

		public RFLearningOperator(){
			super();
		}

		// ----------------------------------------------------
		// LOAD SKETCH INTO MEMORY
		// ----------------------------------------------------

		/**
		 * read the sketch, put it into a sketch structure
		 * @param sketch
		 * @param output
		 * @throws Exception
		 */
		@Override
		public void mapPartition(Iterator<String> sketch, Collector<Tuple1<String>> output) throws Exception {
			this.output = output;

			while(sketch.hasNext()){
				String[] fields = sketch.next().split(",");
				String sketchtype = fields[0];


				if(sketchtype.compareTo("node-sketch") == 0 ) {
					String sampleId = fields[1];
					String label = fields[2];
					String featureId = fields[3];
					Double featureVal = Double.parseDouble(fields[4]);
					Double splitCandidate = Double.parseDouble(fields[5]);

					qj.add(sampleId + label);

					if (featureVal < splitCandidate) {
						qjL.add(sampleId + featureId + splitCandidate);
					} else {
						qjR.add(sampleId + featureId + splitCandidate);
					}
				}


				if(sketchtype.compareTo("split-candidate") == 0 ) {
					Integer featureId = Integer.parseInt(fields[1]);
					String[] features = fields[2].split(" ");
					String[] featureList = new String[features.length];
					for( int i=0; i < features.length; i++ ){
						featureList[i] = features[i];
					}//for
					splitCandidates.put(featureId, featureList);
				}

				if(sketchtype.compareTo("sample-sketch") == 0 ) {
					String sampleId = fields[1];
					String label = fields[2];

					baggingTable.add( new Tuple2<Integer, Integer>( Integer.parseInt(sampleId), Integer.parseInt(label) ));

				}
			}

			// now start learning phase
			learn(output);
		}

		// ----------------------------------------------------
		// START LEARNING PHASE
		// ----------------------------------------------------

		@Override
		public void learn(Collector<Tuple1<String>> output) {
			for( int tree=0; tree < NUMBER_TREES_PER_NODE; tree++ ){
				List<Integer> featureSpace = new ArrayList<Integer>();
				for(int i=0; i < RFSketching.NUMBER_FEATURES; i++ ) featureSpace.add(i);
				//int[] baggingTable = createBaggingtable(RFSketching.NUMBER_SAMPLES);

				List<Integer> features = selectRandomFeatures(featureSpace, SELECT_FEATURES_PER_NODE );

				splitNode( baggingTable, features, featureSpace, tree, BigInteger.valueOf(0) );
			}
		}

		/**
		 * this method do all the magic stuff, find the best split, create new nodes and recursively split these
		 * nodes again until a stopping criterion is reached
		 *
		 * @param baggingTable node bagging table
		 * @param features randomly selected features to find the next best split
		 * @param featureSpace
		 * @param treeId
		 * @param nodeId
		 */
		public void splitNode( List<Tuple2<Integer,Integer>> baggingTable, List<Integer> features, List<Integer> featureSpace, long treeId, BigInteger nodeId ) {

			// check for stopping criterion


			// build bagging table LEFT and RIGHT

		}

		/*public SplitCandidate node_feature_distribution( int feature, String candidate, TreeNode node ){


		}*/

		/**+
		 * decide whether the current split should not split again
		 * @param bestSplit
		 * @return
		 */
		public boolean isStoppingCriterion( SplitCandidate bestSplit ) {
			if (bestSplit.splitLeft == 0 || bestSplit.splitRight == 0 || bestSplit.splitLeft < minNrOfSplitItems || bestSplit.splitRight < minNrOfSplitItems) {
				return true;
			} else {
				return false;
			}
		}

		/**
		 * select num random features from the feature space
		 * @param featureSpace
		 * @param num
		 * @return
		 */
		public List<Integer> selectRandomFeatures(final List<Integer> featureSpace, int num ){
			List<Integer> tmpFeatureSpace = new ArrayList<Integer>(featureSpace);
			List<Integer> features= new ArrayList<Integer>();
			for( int f=0; f < num; f++ ){
				int feature = Random.nextInt(tmpFeatureSpace.size());
				features.add( tmpFeatureSpace.get(feature) );
				tmpFeatureSpace.remove(feature);
			}
			return features;
		}


		/**
		 * TreeNode
		 */
		class TreeNode implements Serializable {
			public int treeId;
			public BigInteger nodeId;
			public List<Integer> features;
			public List<Integer> featueSpace;
			public int featureSplit;
			public double featureSplitValue;
			public int label;
			public List<Tuple2<Integer,Integer>> baggingTable;

			public TreeNode( int treeId,
							 BigInteger nodeId,
							 List<Integer> features,
							 List<Integer> featueSpace,
							 int featureSplit,
							 double featureSplitValue,
							 int label,
							 List<Tuple2<Integer,Integer>> baggingTable) {
				this.treeId = treeId;
				this.nodeId = nodeId;
				this.features = features;
				this.featueSpace = featueSpace;
				this.featureSplit = featureSplit;
				this.featureSplitValue = featureSplitValue;
				this.label = label;
				this.baggingTable = baggingTable;
			}
		}


		/**
		 * Split candidate
		 */
		class SplitCandidate implements Serializable {

			public double tau = 0.5;

			public int feature;
			public String candidate;
			public int totalSamples;
			public int splitLeft;
			public int splitRight;
			public List<Double> qj;
			public List<Double> qjL;
			public List<Double> qjR;

			public SplitCandidate(	int feature,
									String candidate,
									int totalSamples,
									int splitLeft,
									int splitRight,
								  	List<Double> qj,
									List<Double> qjL,
									List<Double> qjR){
				this.feature = feature;
				this.candidate = candidate;
				this.totalSamples = totalSamples;
				this.splitLeft = splitLeft;
				this.splitRight = splitRight;
				this.qj = qj;
				this.qjL = qjL;
				this.qjR = qjR;
			}

			/**
			 * compute for a specific split candidate the split quality
			 * @param tau weighting of the left and right split
			 * @param qj overall node label distribution
			 * @param qL node label distribution for the left side using a specific split candidate
			 * @param qR node label distribution for the right side using a specific split candidate
			 */
			public double quality_function(Double tau, List<Double> qj, List<Double> qL, List<Double> qR ){
				return impurity(qL) - tau * impurity(qL) - (1 - tau) * impurity(qR);
			}

			public double quality_function(){
				return quality_function(tau, qj, qjL, qjR );
			}

			/**
			 * compute the impurity of a specific node split by using e.g. the gini coefficient. there are
			 * still more possibilities like entropy
			 * @param q
			 * @return
			 */
			public double impurity( List<Double> q ) {
				return gini(q);
			}

			/**
			 * comutes the gini coefficient of a node distribution
			 * @param q
			 * @return
			 */
			public double gini( List<Double> q){
				double result = 1;
				for(Double item : q ) result -= item*item;
				return result;
			}


			/**
			 * comutes the entropy of a node distribution
			 * @param q
			 * @return
			 */
			public double entropy(List<Double> q){
				double result = 0;
				for( Double item : q ) result += item*Math.log(item);
				return -result;
			}


		}
	}
}
