package main.scala.tu_berlin.bigdata_sketching.algoritms.ml.random_forest.stratosphere

import org.apache.log4j.Level
import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.client.LocalExecutor
import eu.stratosphere.api.common._
import main.scala.tu_berlin.bigdata_sketching.algoritms._
import main.scala.tu_berlin.bigdata_sketching.algoritms.sketches.DistrubutedSketch

class RFSketch  extends Program with ProgramDescription with Serializable {

  override def getDescription() = {
    "Usage: [inputPath] [outputPath] ([number_trees])"
  }

  override def getPlan(args: String*) = {

    val ds = new DistrubutedSketch(scala.math.pow(10, -8), 0.0000005, 10)
    val num_features = 784
    val max_bins = 10
    val input = TextFile( args(0) )
    val outputFile = args(1)


    val features =input
      .map( l => l.split(" "))
      .map( l => (l(0).toInt, l(1).toInt, l.tail.tail ) )
      .flatMap( x => x._3.zipWithIndex.map( f => (f._2.toInt /*featureIndex*/ , (x._1 /*index*/, x._2 /*label*/, f._1.toDouble /*feature val*/ ))))

    val split_candidates = features
      .groupBy( x => x._1 )
      .reduceGroup( x => {
        val buffered = x.buffered
        val keyValues = buffered.head
        val feature_index = keyValues._1
        val histogram = new Histogram(feature_index, max_bins)
        buffered.foreach( x => histogram.update(x._2._3) )
        (feature_index, histogram.uniform(max_bins).mkString(" ") )
      })
    .filter(x=>x._2.trim.length>0)

    val sketch = features
      .join(split_candidates)
      .where({ x => x._1 })
      .isEqualTo { y => y._1 }
      .map({ (feature, candidates) => (feature._1 /*feautreIndex*/, feature._2._1 /*sampleIndex*/, feature._2._2 /*label*/, feature._2._3.toDouble /*featureVal*/, candidates._2 /*candidates*/ ) })
      .flatMap({ case (f,s,l,v,cand) => cand.split(" ").map( c => (f,s,l,v,c.toDouble,v<=c.toDouble,v>c.toDouble))})

      .reduceAll( x => {
        val left_sketch = ds.new_partial_sketch
        val right_sketch = ds.new_partial_sketch

        val buffered = x.buffered

        left_sketch.alloc
        right_sketch.alloc

        buffered.foreach({ case (f,s,l,v,c,left,right) =>
          if(left) left_sketch.update("key_"+f+s+l+c, 1)
          if(right) right_sketch.update("key_"+f+s+l+c, 1)
        })

      (left_sketch.toString,  right_sketch.toString )
    })
    .flatMap( x => List(x._1, x._2) )


    val sinkSplitCandidates = split_candidates.write( outputFile+"_splitCandidates", CsvOutputFormat())
    val sinkSketch = sketch.write( outputFile+"_sketch", CsvOutputFormat())

    new ScalaPlan(Seq(sinkSplitCandidates, sinkSketch ))
  }
}


object RFSketchingphase {
  def main(args: Array[String]) {
    System.out.println("start")
    val plan = new RFSketch().getPlan("/home/kay/normalized_small.txt", "/home/kay/Desktop/Masterarbeit/results/stratosphere")
    val localExecutor = new LocalExecutor();
    localExecutor.start()
    localExecutor.executePlan(plan)
    System.exit(0)
  }
}
