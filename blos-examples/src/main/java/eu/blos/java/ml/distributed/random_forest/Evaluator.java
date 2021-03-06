package eu.blos.java.ml.distributed.random_forest;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.flink.api.common.functions.GroupReduceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.util.Collector;

import java.math.BigInteger;
import java.util.*;

@Deprecated
public class Evaluator {

	private static final BigInteger TWO = new BigInteger("2");

	private static final Log LOG = LogFactory.getLog(Evaluator.class);

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

		DataSet<Tuple1<String>> treeNodes =
			treeFile.map(new MapFunction<String, Tuple2<Integer,String>>() {
				@Override
				public Tuple2<Integer,String> map(String nodedata ) throws Exception {
					String[] fields = nodedata.split(",");
					Integer treeId = new Integer(fields[0]);
					return new Tuple2<Integer, String>(treeId, nodedata );
				}
			})
			.groupBy(0)
			.reduceGroup(new GroupReduceFunction<Tuple2<Integer, String>, Tuple1<String>>() {
				@Override
				public void reduce(Iterable<Tuple2<Integer, String>> nodes, Collector<Tuple1<String>> output) throws Exception {
					String tree="";
					Iterator<Tuple2<Integer,String>> it = nodes.iterator();
					while (it.hasNext()) {
						Tuple2<Integer, String> n = it.next();
						tree+=n.f1; if(it.hasNext()) tree+=";";
					}
					output.collect(new Tuple1<String>(tree));
				}
			});

		DataSet<Tuple3<Integer, Integer, Integer>> treeEvaluations = testSamples
					.cross(treeNodes)
					.map(new MapFunction<Tuple2<String, Tuple1<String>>, Tuple3<Integer, Integer, Integer>>() {
						@Override
						public Tuple3<Integer, Integer, Integer> map(Tuple2<String, Tuple1<String>> sampleTree) throws Exception {
							String line = sampleTree.f0;
							String tree = sampleTree.f1.f0;
							String[] nodes = tree.split(";");
							String label = line.split(" ")[1];

							String[] sampleValues = line.split(" "); // smapleValues
							Integer labelVote = -1;
							BigInteger currentNodeId = BigInteger.ZERO;
							Integer index = Integer.parseInt(sampleValues[0]);
							int featureSplit = -1;
							Double featureSplitValue=0.0;

							// navigate through the tree and find the class label
							do {
								// find next node. First node is zero.
								boolean found=false;

								for( String node : nodes ){
									String[] nodeFields = node.split(",");
									BigInteger nodeId = new BigInteger( nodeFields[1] );

									if (nodeId.equals(currentNodeId)) {
										labelVote = Integer.parseInt(nodeFields[4]);
										if (labelVote == -1) {
											featureSplit = Integer.parseInt(nodeFields[2]);
											featureSplitValue = Double.parseDouble(nodeFields[3]);
										}//if
										//System.out.println("found => "+labelVote );
										found=true;
										break;
									}//if
								}//for

								if(!found){
									break;
								} else {
									if( Double.parseDouble(sampleValues[featureSplit+2]) <= featureSplitValue ){
									}
								}

								if(labelVote == -1 ){
									currentNodeId = currentNodeId.add(BigInteger.ONE).multiply(TWO);

									// left child
									if( Double.parseDouble(sampleValues[featureSplit+2]) <= featureSplitValue ){
										currentNodeId = currentNodeId.subtract(BigInteger.ONE);
									}
								}//if

							} while(labelVote == -1 );

							return new Tuple3<Integer, Integer, Integer>(index, labelVote, Integer.parseInt(label));
						}
					});


		// forest evaluation
		DataSet<Tuple2<Integer,Integer>> forestEvaluations = treeEvaluations
			.groupBy(0)
			.reduceGroup(
					new GroupReduceFunction<Tuple3<Integer, Integer, Integer>, Tuple2<Integer,Integer>>(){

						@Override
						public void reduce(Iterable<Tuple3<Integer, Integer, Integer>> prediction, Collector<Tuple2<Integer, Integer>> output) throws Exception {
							Iterator<Tuple3<Integer, Integer, Integer>> it = prediction.iterator();
							Map<Integer, Integer> vote = new HashMap<Integer, Integer>();
							int bestVoteLabel=-1;
							int bestVoteCount=0;
							int totalVotes=0;
							int sampleId=0;
							while( it.hasNext() ){
								Tuple3<Integer, Integer, Integer> p = it.next();
								sampleId = p.f0;
								if(vote.containsKey(p.f1)) {
									vote.put(p.f1, vote.get(p.f1)+1 );
								} else {
									vote.put(p.f1, 1 );
								}

								if( vote.get(p.f1) > bestVoteCount ){
									bestVoteLabel = p.f1;
									bestVoteCount = vote.get(p.f1);
								}

								totalVotes++;
							}//while

							output.collect(new Tuple2<Integer,Integer>(sampleId,  bestVoteLabel));
						}
					}
			);


		// emit result
		if(Builder.fileOutput) {
			forestEvaluations.writeAsCsv(outputFile, "\n", " ", FileSystem.WriteMode.OVERWRITE );

		} else {
			forestEvaluations.print();
		}

		// execute program
		env.execute("Evaluation phase");
	}
}
