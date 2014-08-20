package eu.blos.java.ml.random_forest;

import eu.stratosphere.api.java.tuple.Tuple2;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.List;

public class TreeNode implements Serializable {
			public int treeId;
			public BigInteger nodeId;
			public List<Integer> features;
			public List<Integer> featureSpace;
			public Integer featureSplit;
			public String featureSplitValue;
			public int label;
			public List<Tuple2<Integer,Integer>> baggingTable;

			public TreeNode(){}

			public TreeNode( int treeId,
							 BigInteger nodeId,
							 List<Integer> features,
							 List<Integer> featureSpace,
							 Integer featureSplit,
							 String featureSplitValue,
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

			public String toString(){
				return treeId + "," + nodeId + "," + featureSplit + "," + featureSplitValue + "," + label+", "+ (baggingTable!= null ? baggingTable.size() : "null");
			}
		}