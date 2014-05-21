package main.scala.tu_berlin.bigdata_sketching.examples.local

import main.scala.tu_berlin.bigdata_sketching.algoritms.ml.random_forest.local.{TreeNode, DecisionTreeUtils, RandomDecisionTree, RFSketchingPhase}


object BuildRandomForest {

  def main(args: Array[String]) {
    val num_features = 784
    val candidates = 2
    val num_samples = 600000
    val num_labels = 10

    // false positive in percentage
    val p = 0.4


    // sketching phase
    val sketching = new RFSketchingPhase(num_features, candidates, num_samples, num_labels, p )
    val sketch = sketching.build_sketch("/home/kay/normalized_big.txt")

    // initial all features allowed
    val features = List.range(0,num_features).toArray

    val pool = java.util.concurrent.Executors.newFixedThreadPool(4)


    // now build random decision trees
    for( tree <- 0 until 50 ) {
      pool.execute(
        new Runnable {
          def run {
            val rdt = new RandomDecisionTree(sketch, 10, "/home/kay/rf/rf_output_60k_small_sketch/tree_"+tree)

            val featureSpace = DecisionTreeUtils.generateFeatureSubspace(10, features.toBuffer )
            val baggingTable = DecisionTreeUtils.generateBaggingTableFromList(num_samples, sketch.samples_labels ).groupBy( x => (x._1,x._2) ).map( x=> (x._1._1,x._2.size,x._1._2 )).toArray
            val root = new TreeNode(tree, 0, features, featureSpace, -1, -1, -1, baggingTable)

            rdt.build_tree(root)
            rdt.close
          }
        })
    }//for
  }

}