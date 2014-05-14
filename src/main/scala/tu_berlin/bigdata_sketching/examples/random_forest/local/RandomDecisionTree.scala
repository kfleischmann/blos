package main.scala.tu_berlin.bigdata_sketching.examples.random_forest.local
import collection.mutable.HashMap
case class TreeNode ( treeId : Long,
                      nodeId : Long,
                      // list of features total available for this level
                      val features : Array[Int],
                      // list of features left for random feature-selection (m) - subset of "features"
                      featureSpace : Array[Int],
                      // -1 if not set
                      splitFeatureIndex : Int,
                      // -1 if not set
                      splitFeatureValue : Double,
                      // -1 if not set
                      label : Int
                      )

case class BestSplit()

class RandomDecisionTree(val sketch : RFSketch ) {

  def build_tree(random_subspace : Int, node : TreeNode, baggingTable : Array[Int] ) = {

  }

  def impurity(q: List[Double]) = {
    gini(q)
    //entropy(q)
  }

  def gini(q: List[Double]) = {
    1.0 - q.map(x => x * x).sum.toDouble
  }

  def entropy(q: List[Double]) = {
    -q.map(x => x * Math.log(x)).sum
  }

  def quality_function(tau: Double, q: List[Double], qL: List[Double], qR: List[Double]) = {
    impurity(qL) - tau * impurity(qL) - (1 - tau) * impurity(qR);
  }

  def isStoppingCriterion = false

}
