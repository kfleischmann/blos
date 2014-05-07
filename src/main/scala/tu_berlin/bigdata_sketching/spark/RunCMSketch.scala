package main.scala.tu_berlin.bigdata_sketching.spark

import org.apache.spark.SparkContext
import main.scala.tu_berlin.bigdata_sketching.sketches.DistrubutedSketch
import main.scala.tu_berlin.bigdata_sketching.utils.Histogram

object RunCMSketch {
  def test = {
    val ds = new DistrubutedSketch( scala.math.pow(10,-7), 0.005, 10  )

    val s = ds.new_partial_sketch
    val s1 = ds.new_partial_sketch

    s.alloc
    s1.alloc

    val t = 100

    println( s.size.toDouble*4.0/1024.0/1024.0 )

    for( i <- 0 until t ){
      s1.update( "hallo", 1 )

      s.update( i.toString, 1 )
      s1.update( i.toString, 1 )
    }
    var sg = ds.combine( s, s1 )
    println( sg.get_heap.clone().dequeueAll.iterator.toList.toString )
  }


  def main(args: Array[String]) {
    val ds = new DistrubutedSketch(scala.math.pow(10, -7), 0.005, 10)
    val sketch = ds.new_partial_sketch
    val num_features = 674
    val max_bins = 10
    val histograms = new Array[Histogram](num_features)
    for (i <- 0 until num_features) {
      histograms(i) = new Histogram(i, max_bins)
    }

    sketch.alloc


    val sc = new SparkContext("local", "SparkSketches", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)
    val lines = sc.textFile("hdfs://localhost:9000/normalized_full.txt")


    println("samples count " + lines.count())

    lines.map( l => l.split(" ")).map( l => (l(0).toInt, l(1).toInt, l.tail.tail ) )
      .foreach( x => {
        val index = x._1
        val label = x._2
        x._3.take(num_features).zipWithIndex.foreach( feature => histograms(feature._2).update(feature._1.toDouble ) )
    })

    for (i <- 0 until num_features) {
      histograms(i).print
    }

    lines.saveAsTextFile("hdfs://localhost:9000/out3")
  }
}
