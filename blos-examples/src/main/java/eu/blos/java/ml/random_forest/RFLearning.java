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
						sketchDataPath + "/" + RFSketching.PATH_OUTPUT_SKETCH_BAGGINGTABLE
						},
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
		private BloomFilter sketch_qj = new BloomFilter(2^31-1, 1000000 );

		// Knowlege about the feature locations according to the different candidates.
		// Request qjL(s, f, c) -> {0,1}
		private BloomFilter sketch_qjL = new BloomFilter(2^31-1, 1000000 );

		// Knowlege about the feature locations according to the different candidates.
		// Request qjR(s, f, c) -> {0,1}
		private BloomFilter sketch_qjR = new BloomFilter(2^31-1, 1000000 );

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

					sketch_qj.add(sampleId + label);

					if (featureVal < splitCandidate) {
						//sketch_qjL.add( (""+sampleId + featureId + ""+splitCandidate).getBytes() );
					} else {
						//sketch_qjR.add( (""+sampleId + featureId + ""+splitCandidate).getBytes());
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
				BigInteger nodeId = BigInteger.valueOf(0);
				List<Integer> features = selectRandomFeatures(featureSpace, SELECT_FEATURES_PER_NODE );
				int featureSplit = -1;
				double featureSplitValue = -1;
				int label = -1;


				TreeNode node = new TreeNode(tree, nodeId, features, featureSpace, featureSplit, featureSplitValue, label,  baggingTable );

				build_tree( node );
			}
		}

		/**
		 *
		 * @param node
		 */
		public void build_tree( TreeNode node ) {
			List<SplitCandidate> splits = new ArrayList<SplitCandidate>();

			for( Integer feature : node.features ){
				if( this.splitCandidates.containsKey(feature)){
					String[] candidates = this.splitCandidates.get(feature);
					for( String candidate : candidates ) {

						SplitCandidate split = node_feature_distribution( feature, candidate, node );
						splits.add( split );

						System.out.println(split.candidate+" "+split.feature+" "+" "+split.splitLeft+","+split.splitRight+" "+split.quality() );
					}
				}
			}
		}


		/**
		 *
		 * @param feature
		 * @param candidate
		 * @param node
		 * @return
		 */
		public SplitCandidate node_feature_distribution( int feature, String candidate, TreeNode node ){
			Double[] qj  = new Double[RFSketching.NUMBER_LABELS];
			Double[] qjL = new Double[RFSketching.NUMBER_LABELS];
			Double[] qjR = new Double[RFSketching.NUMBER_LABELS];

			for(int i=0; i < RFSketching.NUMBER_LABELS; i++ ){
				qj[i] = new Double(0);
				qjR[i] = new Double(0);
				qjL[i] = new Double(0);
			}


			int totalSamples = node.baggingTable.size();
			int splitLeft = 0;
			int splitRight = 0;

			for( Tuple2<Integer,Integer> sample : node.baggingTable ) {
				qj[sample.f1.intValue()]++;


				if( this.sketch_qjL.contains( (""+sample.f0+feature+candidate).getBytes()) ){
					qjL[sample.f1.intValue()]++;
					splitLeft++;
				}

				if( this.sketch_qjR.contains( (""+sample.f0+feature+candidate).getBytes()) ){
					qjR[sample.f1.intValue()]++;
					splitRight++;
				}
			}

			for(int i=0; i < RFSketching.NUMBER_LABELS; i++ ){
				qj[i] = qj[i] / totalSamples;
				qjR[i] = qjR[i] / splitRight;
				qjL[i] = qjL[i] / splitLeft;
			}

			return new SplitCandidate(
					feature,
					candidate,
					totalSamples,
					splitLeft,
					splitRight,
					new ArrayList<Double>(Arrays.asList(qj)),
					new ArrayList<Double>(Arrays.asList(qjL)),
					new ArrayList<Double>(Arrays.asList(qjR))
			);
		}

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
			public List<Integer> featureSpace;
			public int featureSplit;
			public double featureSplitValue;
			public int label;
			public List<Tuple2<Integer,Integer>> baggingTable;

			public TreeNode( int treeId,
							 BigInteger nodeId,
							 List<Integer> features,
							 List<Integer> featureSpace,
							 int featureSplit,
							 double featureSplitValue,
							 int label,
							 List<Tuple2<Integer,Integer>> baggingTable) {
				this.treeId = treeId;
				this.nodeId = nodeId;
				this.features = features;
				this.featureSpace = featureSpace;
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

			/**
			 * compute the quality of this split
			 * @return split quality
			 */
			public double quality(){
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
