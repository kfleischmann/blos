package main.scala.tu_berlin.bigdata_sketching.examples.random_forest.local


object RandomForest {

  def main(args: Array[String]) {
    val num_features = 784
    val candidates = 10
    val num_samples = 1000

    // false positive in percentage
    val p = 0.01


    // sketching phase
    val sketching = new RFSketchingPhase(num_features, candidates, num_samples, p )
    val sketch = sketching.build_sketch("/home/kay/normalized_very_small.txt")


  }
}
