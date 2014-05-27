package bigdata_sketching.main.scala.algorithms.ml.random_forest.local


import scala.collection.mutable.Buffer
import scala.util.Random
import java.io.File

object DecisionTreeUtils {
  def generateFeatureSubspace(randomCount : Int, maxRandomNumber : Int) : Array[Int] = {
    var features = Buffer[Int]();
    // Generate an arrayList of all Integers
    for(i <- 0 until maxRandomNumber){
      features += i;
    }
    generateFeatureSubspace(randomCount, features)
  }

  def generateFeatureSubspace(randomCount : Int, features : Buffer[Int]) : Array[Int] = {
    var arr : Array[Int] = Array()
    arr = Array(randomCount)
    arr = Array.fill(randomCount)(0)
    val random = new Random()
    for(i <- 0 until randomCount){
      arr(i)=features.remove(random.nextInt(features.length));
    }
    arr
  }

  // contains the id,label
  def generateBaggingTableFromList(size : Int, list : Array[(Int,Int)] ) : Array[(Int,Int)] = {
    var arr : Array[(Int,Int)] = Array()
    arr = Array((size,size))
    arr = Array.fill(size)((0,-1))
    val maxRandomNumber = list.size
    for (i <- 0 until size){
      arr(i) = list(Random.nextInt(maxRandomNumber))
    }
    arr
  }

  def generateBaggingTable(size : Int, maxRandomNumber : Int ) : Array[Int] = {
    var arr : Array[Int] = Array()
    arr = Array(size)
    arr = Array.fill(size)(0)
    for (i <- 0 until size){
      arr(i) = Random.nextInt(maxRandomNumber)
    }
    arr
  }

  var preParseURIForLocalFileSystem = false

  /**
   * Useful when testing on a Windows system
   */
  def preParseURI(uri : String) : String = {
    if (preParseURIForLocalFileSystem)
      new File(uri).toURI().toString()
    else
      uri
  }
}
