package eu.blos.java.ml.random_forest;

import eu.stratosphere.api.java.DataSet;
import eu.stratosphere.api.java.ExecutionEnvironment;
import eu.stratosphere.api.java.functions.GroupReduceFunction;
import eu.stratosphere.api.java.functions.MapFunction;
import eu.stratosphere.api.java.tuple.Tuple2;
import eu.stratosphere.api.java.tuple.Tuple3;
import eu.stratosphere.util.Collector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class RFEvaluation {
	private static final BigInteger TWO = new BigInteger("2");
	private static final Log LOG = LogFactory.getLog(RFEvaluation.class);

	/**
	 * evaluate the testset on the learned trees. write the result back to outputPath
	 *
	 * @param env
	 * @param treesPath
	 * @param inputFile
	 * @param outputFile
	 * @throws Exception
	 */
	public static void evaluate(final ExecutionEnvironment env, String treesPath, String inputFile, String outputFile ) throws Exception {
		DataSet<String> testSamples = env.readTextFile(inputFile);
		DataSet<String> treeFile = env.readTextFile(treesPath);

		DataSet<Tuple2<Integer, List<TreeNode>>> treeNodes =
			treeFile.map(new MapFunction<String, Tuple2<Integer,TreeNode>>() {
				@Override
				public Tuple2<Integer,TreeNode> map(String nodedata ) throws Exception {
					String[] fields = nodedata.split(",");
					Integer treeId = new Integer(fields[0]);
					BigInteger nodeId = new BigInteger(fields[1]);
					Integer featureSplit = Integer.parseInt(fields[2]);
					String featureSplitValue = fields[3];
					int label = Integer.parseInt(fields[4]);

					TreeNode treeNode = new TreeNode(treeId, nodeId, null, null, featureSplit, featureSplitValue, label, null);

					return new Tuple2<Integer, TreeNode>(treeId, treeNode );
				}
			})
			.groupBy(0)
			.reduceGroup(new GroupReduceFunction<Tuple2<Integer, TreeNode>, Tuple2<Integer, List<TreeNode>>>() {
				@Override
				public void reduce(Iterator<Tuple2<Integer, TreeNode>> nodes, Collector<Tuple2<Integer, List<TreeNode>>> output) throws Exception {
					List<TreeNode> treeNodes = new ArrayList<TreeNode>();
					Integer treeId = null;
					while (nodes.hasNext()) {
						Tuple2<Integer, TreeNode> n = nodes.next();
						treeNodes.add(n.f1);
						if (treeId == null) treeId = n.f0;
					}


					output.collect(new Tuple2<Integer, List<TreeNode>>(treeId, treeNodes));
				}
			});


		DataSet<Tuple3 <Integer, Integer, Integer>> treeEvaluations =
		testSamples.cross(treeNodes).map( new MapFunction<Tuple2<String, Tuple2<Integer, List<TreeNode>>>, Tuple3 <Integer, Integer, Integer>>() {
			@Override
			public Tuple3 <Integer, Integer, Integer> map(Tuple2<String, Tuple2<Integer, List<TreeNode>>> line_tree) throws Exception {
				String[] sampleValues = line_tree.f0.split(" "); // smapleValues
				Integer labelVote = -1;
				BigInteger currentNodeId = BigInteger.ZERO;
				Integer index = Integer.parseInt(sampleValues[0]);



				do {
					TreeNode currentNode = null;

					// find next node. First node is zero.
					for( TreeNode node : line_tree.f1.f1 ) if( node.nodeId.equals(currentNodeId)) labelVote = node.label;

					if(labelVote == -1 ){
						// right child
						currentNodeId = currentNode.nodeId.add(BigInteger.ONE).multiply(TWO);

						// left child
						if( Double.parseDouble(sampleValues[currentNode.featureSplit+2]) <= Double.parseDouble(currentNode.featureSplitValue) ){
							currentNodeId.subtract(BigInteger.ONE);
						}
					}

				}while(labelVote == -1 );


				return new Tuple3 <Integer, Integer, Integer>(index,labelVote, 0 );
			}
		});


		// forest evaluation



		// emit result
		if(RFBuilder.fileOutput) {
			//cout.writeAsCsv(outputFile, "\n", ",", FileSystem.WriteMode.OVERWRITE );

		} else {
			//cout.print();
		}

		// execute program
		env.execute("Evaluation phase");

	}
}
