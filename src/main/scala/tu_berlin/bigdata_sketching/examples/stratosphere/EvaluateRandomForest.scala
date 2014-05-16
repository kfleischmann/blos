package main.scala.tu_berlin.bigdata_sketching.examples.stratosphere

import main.scala.tu_berlin.bigdata_sketching.algoritms.ml.random_forest.stratosphere.RandomForestBuilder


object EvaluateRandomForest{
  def main(args: Array[String]) {
    val rf = new RandomForestBuilder().eval("file:///home/kay/normalized_test.txt", "file:///home/kay/rf/rf_output_60k_small_sketch/", "/home/kay/output")
  }
}
