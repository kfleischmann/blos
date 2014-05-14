package main.scala.tu_berlin.bigdata_sketching.examples.random_forest.local

import org.apache.hadoop.util.bloom.Key

case class SplitCandidate( feature : Int, candidate : Double, total : Int , split_left : Int, split_right : Int, qj : List[Double], qjL : List[Double], qjR : List[Double] ) {
  val tau = 0.5
  val quality = quality_function(tau, qj, qjL, qjR )

  def get_quality = quality

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
}

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
                      label : Int,
                      // baggingtable
                      baggingTable : Array[Int]
                      ) {
  override def toString = {
    treeId + "," + nodeId + "," + splitFeatureIndex + "," + splitFeatureValue + "," + label
  }
}

class RandomDecisionTree(val sketch : RFSketch, minNrOfSplitItems : Int, out : String ) {

  def majority_class(candidate : SplitCandidate, node : TreeNode ) : (Int,Int) = {
    val labels = Array.fill[Int](sketch.num_labels)(0)

    for( sample <- node.baggingTable ) {
      for (label <- 0 until sketch.num_labels) {
        val keyqjL = "key_"+sample+"_"+candidate.feature+"_"+candidate.candidate+"_"+label+"_L"
        if( sketch.filter.membershipTest(new Key(keyqjL.getBytes())) ) {
          labels(label) += 1
        }
        val keyqjR = "key_"+sample+"_"+candidate.feature+"_"+candidate.candidate+"_"+label+"_R"
        if( sketch.filter.membershipTest(new Key(keyqjR.getBytes())) ) {
          labels(label) += 1
        }
      }
    }

    labels.toList.zipWithIndex.max
  }

  def build_bagging_table(candidate : SplitCandidate, node : TreeNode ) = {
    var left : scala.collection.mutable.Buffer[Int] = scala.collection.mutable.Buffer[Int]()
    var right : scala.collection.mutable.Buffer[Int] = scala.collection.mutable.Buffer[Int]()
    for( sample <- node.baggingTable ) {
      for (label <- 0 until sketch.num_labels) {
        val keyqjL = "key_"+sample+"_"+candidate.feature+"_"+candidate.candidate+"_"+label+"_L"
        if( sketch.filter.membershipTest(new Key(keyqjL.getBytes())) ) {
          left += sample
        }
        val keyqjR = "key_"+sample+"_"+candidate.feature+"_"+candidate.candidate+"_"+label+"_R"
        if( sketch.filter.membershipTest(new Key(keyqjR.getBytes())) ) {
          right += sample
        }
      }
    }
    (left.toArray,right.toArray)
  }

  def node_feature_distribution( feature : Int, candidate : Double, node : TreeNode ) : SplitCandidate = {
    val qj = Array.fill[Int](sketch.num_labels)(0)
    val qjL = Array.fill[Int](sketch.num_labels)(0)
    val qjR = Array.fill[Int](sketch.num_labels)(0)

    var numqj = 0
    var numqjL = 0
    var numqjR = 0

    for( sample <- node.baggingTable ) {
      for (label <- 0 until sketch.num_labels) {
        val keyqj = "key_" + sample+"_" + label
        if( sketch.filter.membershipTest(new Key(keyqj.getBytes())) ){
          qj(label) = qj(label) + 1
          numqj+=1
        }
        val keyqjL = "key_"+sample+"_"+feature+"_"+candidate+"_"+label+"_L"
        if( sketch.filter.membershipTest(new Key(keyqjL.getBytes())) ) {
          qjL(label) = qjL(label)+1
          numqjL+=1
        }
        val keyqjR = "key_"+sample+"_"+feature+"_"+candidate+"_"+label+"_R"
        if( sketch.filter.membershipTest(new Key(keyqjR.getBytes())) ) {
          qjR(label) = qjR(label)+1
          numqjR+=1
        }
      }
    }
    val _qj = qj.toList.map(x=>x/numqj.toDouble)
    val _qjL = qjL.toList.map(x=>x/numqjL.toDouble)
    val _qjR = qjR.toList.map(x=>x/numqjR.toDouble)

    new SplitCandidate( feature, candidate,
                        numqj, numqjL, numqjR,
                        _qj, _qjL, _qjR )

  }

  def build_tree( node : TreeNode ) {
    println("split node "+node.treeId+","+node.nodeId)
    val bestSplit = node.featureSpace.flatMap( feature => sketch.candidates(feature).map( candidate => node_feature_distribution(feature,candidate, node) ) ).maxBy( x => x.get_quality )

    if(!isStoppingCriterion(bestSplit)){
      val leftNodeId : Long = ((node.nodeId + 1L) * 2) - 1
      val rightNodeId : Long = ((node.nodeId + 1L) * 2)

      val features = node.features.toList.filter( x => x != bestSplit.feature ).toArray
      val featureSpace = DecisionTreeUtils.generateFeatureSubspace(10, features.toBuffer )

      val baggingTables = build_bagging_table(bestSplit, node )

      val node_left = new TreeNode(node.treeId, leftNodeId.toInt, features, featureSpace, -1, -1, -1, baggingTables._1 )
      val node_right = new TreeNode(node.treeId, rightNodeId.toInt, features, featureSpace, -1, -1, -1, baggingTables._2  )

      val middleNode = new TreeNode(node.treeId, node.nodeId, null, null, bestSplit.feature, bestSplit.candidate, -1, null )
      addNode(middleNode)

      build_tree(node_left)
      build_tree(node_right)
    } else {
      // majority voting
      val label = majority_class(bestSplit, node)._2
      val finalNode = new TreeNode(node.treeId, node.nodeId, null, null, -1, -1, label, null )
      addNode(finalNode)

      println( "finished node with class "+label )
    }
  }

  def isStoppingCriterion( bestSplit : SplitCandidate ) = {
    if( bestSplit.split_left == 0 || bestSplit.split_right == 0 || bestSplit.split_left < minNrOfSplitItems ||  bestSplit.split_right < minNrOfSplitItems ) {
      true
    } else {
      false
    }
  }

  def addNode(node : TreeNode ) {
  }
}
