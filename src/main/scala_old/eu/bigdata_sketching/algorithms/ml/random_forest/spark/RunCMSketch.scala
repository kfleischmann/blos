package bigdata_sketching.main.scala.algorithms.ml.random_forest.spark


import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import bigdata_sketching.main.scala.algorithms.Histogram
import bigdata_sketching.main.scala.algorithms.sketches.DistrubutedSketch

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
    val num_features = 674
    val max_bins = 10

    val sc = new SparkContext("local", "SparkSketches", System.getenv("SPARK_HOME"), SparkContext.jarOfClass(this.getClass).toSeq)
    //val lines = sc.textFile("hdfs://localhost:9000/normalized_full.txt")
    val lines = sc.textFile("/home/kay/normalized_very_small.txt")


    val sketch = ds.new_partial_sketch
    sketch.alloc

    println("samples count " + lines.count())

    val features =  lines .map( l => l.split(" ")).map( l => (l(0).toInt, l(1).toInt, l.tail.tail ) )
                            .flatMap( x => x._3.zipWithIndex.map( f => (f._2.toInt /*featureIndex*/ , (x._1 /*index*/, x._2 /*label*/, f._1.toDouble /*feature val*/ ) ) ) )
                            .groupByKey

    val split_candidates = features
                            .map( x => {
                                val feature_index = x._1
                                val features = x._2
                                val histogram = new Histogram(feature_index, max_bins)
                                features.foreach(value => histogram.update(value._3))
                                (feature_index, histogram.uniform(max_bins) )
                            })

    val joined = features.join(split_candidates)

    joined.saveAsTextFile("/home/kay/Desktop/Masterarbeit/results/tmp/joined")

    features.saveAsTextFile("/home/kay/Desktop/Masterarbeit/results/tmp/features")
    split_candidates.saveAsTextFile("/home/kay/Desktop/Masterarbeit/results/tmp/split_candidates")



      /*.foreach( x => {
        val index = x._1
        val label = x._2
        x._3.take(num_features).zipWithIndex.foreach( feature => histograms(feature._2).update(feature._1.toDouble ) )
    })*/

    /*for (i <- 0 until num_features) {
      histograms(i).print
    }*/

    //lines.saveAsTextFile("hdfs://localhost:9000/out3")
  }
}

