package eu.bigdata_sketching.examples.stratosphere

object EvaluateRandomForest{
  def main(args: Array[String]) {
    val rf = new RandomForestBuilder().eval("file:///home/kay/normalized_test.txt", "file:///home/kay/rf/rf_output_60k_10_trees_singlesketch_test1/", "/home/kay/output")
  }
}
