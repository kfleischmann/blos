package eu.blos.java.ml.random_forest;

import eu.blos.java.algorithms.sketches.BloomFilter;
import eu.blos.java.algorithms.sketches.Sketch;
import eu.blos.java.api.common.LearningFunction;
import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.functions.MapPartitionFunction;
import eu.stratosphere.api.java.tuple.Tuple1;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.core.fs.FileSystem;
import eu.stratosphere.core.fs.Path;
import eu.stratosphere.util.Collector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jblas.util.Random;
import java.io.Serializable;
import java.math.BigInteger;
import java.util.*;


public class RFLearning {
	private static final Log LOG = LogFactory.getLog(RFLearning.class);

	public static int SELECT_FEATURES_PER_NODE = 5;

	public static int NUMBER_NODES = 1;

	public static int NUMBER_TREES = 0;

	public static int NUMBER_TREES_PER_NODE = 0;

	public static int minNrOfSplitItems = 50;

	/**
	 * Run the Learning phase
	 *
	 * @param env
	 * @param sketchDataPath
	 * @param outputTreePath
	 * @param sketches
	 * @param args
	 * @throws Exception
	 */
    public static void learn(final ExecutionEnvironment env, String preprocessedDataPath, String sketchDataPath, String outputTreePath, Sketch[] sketches, String ... args ) throws Exception {

		LOG.info("start learning phase");


		// remember the number of tress
		NUMBER_TREES = Integer.parseInt(args[0]);

		// how many trees per node?
		NUMBER_TREES_PER_NODE = NUMBER_TREES / NUMBER_NODES;

		// prepare environment, distribute the sketches to all nodes and start learning phase
		readSketchesAndLearn(env, new String[]{
						sketchDataPath + "/" + RFPreprocessing.PATH_OUTPUT_SKETCH_NODE+"-left",
						sketchDataPath + "/" + RFPreprocessing.PATH_OUTPUT_SKETCH_NODE+"-right",
						preprocessedDataPath + "/" + RFPreprocessing.PATH_OUTPUT_SKETCH_SPLIT_CANDIDATES,
						preprocessedDataPath + "/" + RFPreprocessing.PATH_OUTPUT_SKETCH_SAMPLE_LABELS
						},
						outputTreePath,
						sketches);
    }

	/**
	 *
	 * @param env
	 * @param sketchSources
	 * @param outputPath
	 * @throws Exception
	 */
	public static void readSketchesAndLearn( final ExecutionEnvironment env, String[] sketchSources, String outputPath, final Sketch[] sketches ) throws Exception {

		LOG.info("start reading sketches into memory ");

		// read sketches into memory
		DataSet<Tuple2<String,String>> SketchDataSet = null;

		for( String source : sketchSources ) {
			if(SketchDataSet == null ){
				SketchDataSet = env.readTextFile( source ).map(new MapSketchType(new Path(source).getName()));
			} else {
				// read new source
				DataSet<Tuple2<String,String>> sketchDataSet = env.readTextFile( source ).map(new MapSketchType(new Path(source).getName()));

				// append reading
				SketchDataSet = SketchDataSet.union( sketchDataSet  );
			}
		}

		// do the learning
		DataSet<Tuple1<String>> trees = SketchDataSet.mapPartition(new RFLearningOperator(sketches)).setParallelism(1);

		// emit result
		if(RFBuilder.fileOutput) {
			trees.writeAsCsv(outputPath, "\n", ",", FileSystem.WriteMode.OVERWRITE);
		} else {
			trees.print();
		}

		// execute program
		env.execute("Learning phase");
	}


	/**
	 *
	 */
	static class MapSketchType extends MapFunction<String, Tuple2<String,String>> implements Serializable {
		private String sketchType;

		public MapSketchType(String sketchType){
			this.sketchType = sketchType;
		}

		@Override
		public Tuple2<String, String> map(String s) throws Exception {
			return new Tuple2<String, String>(this.sketchType, s);
		}
	}


	/**
	 * RandomForest LearningOprator
	 *
	 * this operator reads the sketch into memory built in the previous phase and starts the learning process
	 * output: final trees
	 */
	static class RFLearningOperator extends  MapPartitionFunction<Tuple2<String,String>, Tuple1<String>> implements Serializable, LearningFunction<Tuple1<String>> {
		private static final Log LOG = LogFactory.getLog(RFLearningOperator.class);

		// --------------------------------------------------
		// SKETCH STRUCTURE for the learning phase
		// --------------------------------------------------
		private int BLOOM_FILTER_SIZE=2147483;
		private double PROBABILITY_FALSE_POSITIVE = 0.3;


		// Knowlege about the sample-labels.
		// Request qj(s, l) -> {0,1}
		//private BloomFilter sketch_qj; // = new BloomFilter( PROBABILITY_FALSE_POSITIVE , RFPreprocessing.NUM_SAMPLES );

		// Knowlege about the feature locations according to the different candidates.
		// Request qjL(s, f, c) -> {0,1}
		private BloomFilter sketch_qjL; // = new BloomFilter( PROBABILITY_FALSE_POSITIVE, RFPreprocessing.NUM_SAMPLES* RFPreprocessing.NUM_SAMPLE_FEATURES * RFPreprocessing.HISTOGRAM_SPLIT_CANDIDATES);

		// Knowlege about the feature locations according to the different candidates.
		// Request qjR(s, f, c) -> {0,1}
		private BloomFilter sketch_qjR; // = new BloomFilter( PROBABILITY_FALSE_POSITIVE, RFPreprocessing.NUM_SAMPLES* RFPreprocessing.NUM_SAMPLE_FEATURES * RFPreprocessing.HISTOGRAM_SPLIT_CANDIDATES );

		private Collector<Tuple1<String>> output;

		// Knowlege about all split-canidates computed in the sketching phase
		private Map<Integer, String[]> splitCandidates = new HashMap<Integer, String[]>();

		//private Map<String, String> sampleLabels = new HashMap<String, String>();

		private List<Tuple2<Integer,Integer>> baggingTable = new ArrayList<Tuple2<Integer,Integer>>();

		public RFLearningOperator(Sketch[] sketches){
			super();

			//sketch_qj = (BloomFilter)sketches[0];
			sketch_qjL = (BloomFilter)sketches[1];
			sketch_qjR = (BloomFilter)sketches[2];

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
		public void mapPartition(Iterator<Tuple2<String,String>> sketch, Collector<Tuple1<String>> output) throws Exception {
			this.output = output;

			//sketch_qj.allocate();
			sketch_qjL.allocate();
			sketch_qjR.allocate();

			LOG.info("finished reading sketches into memory");

			System.out.println(sketch_qjL.getBitSetSize());
			System.out.println(sketch_qjR.getBitSetSize());
			int left=0;
			int right=0;

			while(sketch.hasNext()){
				Tuple2<String,String> sketchData = sketch.next();

				String sketchType = sketchData.f0;
				String sketchFields = sketchData.f1;

				// ------------------------------
				// BUILD THE NODE SKETCH
				// ------------------------------

				if(sketchType.compareTo("rf-sketch-left") == 0 ) {
					String[] fields = sketchFields.split(",");
					Long bit = Long.parseLong(fields[0]);
					Long count = Long.parseLong(fields[2]);

					sketch_qjL.setBit( bit.longValue(), true );
					left++;
				}

				if(sketchType.compareTo("rf-sketch-right") == 0 ) {
					String[] fields = sketchFields.split(",");
					Long bit = Long.parseLong(fields[0]);
					Long count = Long.parseLong(fields[2]);

					sketch_qjR.setBit( bit.longValue(), true );
					right++;
				}


				if(sketchType.compareTo("feature-split-candidates") == 0 ) {
					String[] fields = sketchFields.split(",");

					Integer featureId = Integer.parseInt(fields[0]);
					String[] features = fields[1].split(" ");
					String[] featureList = new String[features.length];
					for( int i=0; i < features.length; i++ ){
						featureList[i] = features[i];
					}//for

					splitCandidates.put(featureId, featureList);
				}

				if(sketchType.compareTo("sample-labels") == 0 ) {
					String[] fields = sketchFields.split(",");

					String sampleId = fields[0];
					String label = fields[1];
					baggingTable.add( new Tuple2<Integer, Integer>( Integer.parseInt(sampleId), Integer.parseInt(label) ));
				}

			}

			LOG.info("finished reading sketches into memory");

			// ---------------------------------------
			// START LEARNING PHASE
			// ---------------------------------------


			learn(output);
		}

		// ----------------------------------------------------
		// START LEARNING PHASE
		// ----------------------------------------------------

		@Override
		public void learn(Collector<Tuple1<String>> output) {
			LOG.info("start building trees");


			for( int tree=0; tree < NUMBER_TREES_PER_NODE; tree++ ){
				List<Integer> featureSpace = new ArrayList<Integer>();
				for(int i=0; i < RFPreprocessing.NUM_SAMPLE_FEATURES; i++ ) featureSpace.add(i);
				BigInteger nodeId = BigInteger.valueOf(0);
				List<Integer> features = selectRandomFeatures(featureSpace, SELECT_FEATURES_PER_NODE );
				Integer featureSplit = -1;
				String featureSplitValue = "";
				int label = -1;

				TreeNode node = new TreeNode(tree, nodeId, features, featureSpace, featureSplit, featureSplitValue, label,  baggingTable );
				buildTree(node);
			} //for

		}

		/**
		 * build the tree recursively
		 * @param node
		 */
		public void buildTree( TreeNode node ) {
			List<SplitCandidate> splits = new ArrayList<SplitCandidate>();

			SplitCandidate bestSplit = null;

			for( Integer feature : node.features ){
				if( this.splitCandidates.containsKey(feature)){
					String[] candidates = this.splitCandidates.get(feature);
					for( String candidate : candidates ) {

						SplitCandidate split = computeNodeFeaturDistribution(feature, candidate, node);
						splits.add( split );

						if(bestSplit==null){
							bestSplit = split;
						} else {
							if( split.quality() > bestSplit.quality() ){
								bestSplit = split;
							}
						}

						System.out.println("Split: "+split.featureValue+" "+split.feature+" "+" "+split.splitLeft+","+split.splitRight+" "+split.quality() );

					}//for
				}//for
			}

			System.out.println("bestSplit: "+bestSplit.featureValue+" "+bestSplit.feature+" "+" "+bestSplit.splitLeft+","+bestSplit.splitRight+" "+bestSplit.quality() );

			if(!isStoppingCriterion(bestSplit)){
				Tuple2<List<Tuple2<Integer,Integer>>, List<Tuple2<Integer,Integer>>> baggingTables = createBaggingTable(bestSplit, node );

				BigInteger leftNodeId = node.nodeId.add(BigInteger.ONE).multiply(BigInteger.valueOf(2)).subtract(BigInteger.ONE);
				BigInteger rightNodeId = node.nodeId.add(BigInteger.ONE).multiply(BigInteger.valueOf(2));

				List<Integer> featureSpace = new ArrayList<Integer>(node.featureSpace);

				LOG.debug("featureSpace Before: "+featureSpace.size());
				featureSpace.remove(bestSplit.feature);
				LOG.debug("featureSpace After: "+featureSpace.size());


				TreeNode leftNode = new TreeNode( node.treeId, leftNodeId, selectRandomFeatures(featureSpace, SELECT_FEATURES_PER_NODE ), featureSpace, -1, "", -1,  baggingTables.f0 );
				TreeNode rightNode = new TreeNode( node.treeId, rightNodeId, selectRandomFeatures(featureSpace, SELECT_FEATURES_PER_NODE ), featureSpace, -1, "", -1,  baggingTables.f1 );

				LOG.debug("leftNode:"+leftNode);
				LOG.debug("rightNode:"+rightNode);

				TreeNode middleNode = new TreeNode( node.treeId, node.nodeId, null, null, bestSplit.feature, bestSplit.featureValue, -1, null );

				addNode(middleNode);

				buildTree(leftNode);
				buildTree(rightNode);


			} else {

				// majority voting
				int label = bestSplit.getMajorityLabel();

				TreeNode finalNode = new TreeNode( node.treeId, node.nodeId, null, null, -1, "", label, null );

				addNode(finalNode);

				LOG.debug( "finished node "+finalNode.nodeId+" with class "+label );
			}

		}

		/**
		 *
		 * @param candidate
		 * @param node
		 * @return
		 */
		public Tuple2<List<Tuple2<Integer,Integer>>, List<Tuple2<Integer,Integer>>> createBaggingTable( SplitCandidate candidate, TreeNode node ) {
			List<Tuple2<Integer,Integer>> left = new ArrayList<Tuple2<Integer,Integer>>();
			List<Tuple2<Integer,Integer>> right = new ArrayList<Tuple2<Integer,Integer>>();

			for( Tuple2<Integer,Integer> sample : node.baggingTable ) {
				if( this.sketch_qjL.contains( ""+sample.f0+" "+candidate.feature+" "+candidate.featureValue) ){
					left.add( sample );
				}
				if( this.sketch_qjR.contains( ""+sample.f0+" "+candidate.feature+" "+candidate.featureValue) ){
					right.add( sample );
				}
			}//for
			return new Tuple2<List<Tuple2<Integer,Integer>>, List<Tuple2<Integer,Integer>>>( left, right );
		}

		/**
		 * construct the feature-distribution for a specific node by using the sketches. This is necessary
		 * to compute the splits and find the best one
		 *
		 * @param feature
		 * @param candidate
		 * @param node
		 * @return
		 */
		public SplitCandidate computeNodeFeaturDistribution( int feature, String candidate, TreeNode node ){
			Double[] qj  = new Double[RFPreprocessing.NUM_SAMPLE_LABELS];
			Double[] qjL = new Double[RFPreprocessing.NUM_SAMPLE_LABELS];
			Double[] qjR = new Double[RFPreprocessing.NUM_SAMPLE_LABELS];

			for(int i=0; i < RFPreprocessing.NUM_SAMPLE_LABELS; i++ ){
				qj[i] = new Double(0);
				qjR[i] = new Double(0);
				qjL[i] = new Double(0);
			}

			System.out.println("computeNodeFeaturDistribution");


			int totalSamples = node.baggingTable.size();
			int splitLeft = 0;
			int splitRight = 0;

			for( Tuple2<Integer,Integer> sample : node.baggingTable ) {

				// find the labesl from
				qj[sample.f1.intValue()]++;

				// find the labels from sketch
				/*for(int i=0; i < RFPreprocessing.NUM_SAMPLE_LABELS; i++ ){
					if( this.sketch_qjL.contains( (""+sample.f0+" "+i).getBytes()) ){
						qj[i]++;
					}
				}*/

				//System.out.println("check: "+""+sample.f0+" "+feature+" "+candidate );
				if( this.sketch_qjL.contains( ""+sample.f0+" "+feature+" "+candidate) ){
					qjL[sample.f1.intValue()]++;
					splitLeft++;
				}

				if( this.sketch_qjR.contains( ""+sample.f0+" "+feature+" "+candidate) ){
					qjR[sample.f1.intValue()]++;
					splitRight++;
				}
			}

			for(int i=0; i < RFPreprocessing.NUM_SAMPLE_LABELS; i++ ){
				qj[i] = qj[i] / totalSamples;
				qjR[i] = qjR[i] / totalSamples;
				qjL[i] = qjL[i] / totalSamples;
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
		 * decide if the current split should not split again
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
			List<Integer> tmpFeatureSpace = new ArrayList<Integer>();
			for(Integer i : featureSpace) tmpFeatureSpace.add(i);

			List<Integer> features= new ArrayList<Integer>();
			while(features.size() < num ){
				//for( int f=0; f < num; f++ ){
				Integer feature = tmpFeatureSpace.get( Random.nextInt(tmpFeatureSpace.size()-1));

				if(this.splitCandidates.containsKey(feature) ) {
					features.add(feature);
					tmpFeatureSpace.remove(feature);
				}
			}
			return features;
		}


		/**
		 * add the node to output queue
		 * @param node
		 */
		public void addNode(TreeNode node ){
			output.collect( new Tuple1<String>(node.toString()) );
		}


		/**
		 * TreeNode
		 */


		/**
		 * Split candidate
		 */
		class SplitCandidate implements Serializable {

			public double tau = 0.5;

			public Integer feature;
			public String featureValue;
			public int totalSamples;
			public int splitLeft;
			public int splitRight;
			public List<Double> qj;
			public List<Double> qjL;
			public List<Double> qjR;

			public SplitCandidate(	Integer feature,
									String featureValue,
									int totalSamples,
									int splitLeft,
									int splitRight,
								  	List<Double> qj,
									List<Double> qjL,
									List<Double> qjR){
				this.feature = feature;
				this.featureValue = featureValue;
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

			/**
			 * Label with the highest probabiltiy in this node-split
			 * @return Integer
			 */
			public int getMajorityLabel(){
				int label=-1;
				Double val=0.0;
				for(int i=0; i < qj.size(); i++ ){
					if(val<qj.get(i)){
						label=i; val = qj.get(i);
					}//if
				}//for
				return label;
			}
		}
	}
}
