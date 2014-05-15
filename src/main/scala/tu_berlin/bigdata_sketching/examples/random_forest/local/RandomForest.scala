package main.scala.tu_berlin.bigdata_sketching.examples.random_forest.local

import java.io.FileWriter


object RandomForest {

  def main(args: Array[String]) {
    val num_features = 784
    val candidates = 10
    val num_samples = 60000
    val num_labels = 10

    // false positive in percentage
    val p = 0.01


    // sketching phase
    val sketching = new RFSketchingPhase(num_features, candidates, num_samples, num_labels, p )
    val sketch = sketching.build_sketch("/home/kay/normalized_full.txt")

    val rdt = new RandomDecisionTree(sketch, 10, "/home/kay/rf/rf_output_tree_1p_60k")
    // initial all features allowed
    val features = List.range(0,num_features).toArray


    // now build random decision trees
    for( tree <- 0 until 1 ) {
      val featureSpace = DecisionTreeUtils.generateFeatureSubspace(10, features.toBuffer )
      val baggingTable = DecisionTreeUtils.generateBaggingTable(num_samples, num_samples )
      val root = new TreeNode(tree, 0, features, featureSpace, -1, -1, -1, baggingTable)
      rdt.build_tree(root)
    }//for

    rdt.close
  }
}
