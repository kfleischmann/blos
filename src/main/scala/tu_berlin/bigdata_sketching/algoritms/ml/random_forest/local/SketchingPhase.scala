package main.scala.tu_berlin.bigdata_sketching.algoritms.ml.random_forest.local

import main.scala.tu_berlin.bigdata_sketching.algoritms.sketches._
import main.scala.tu_berlin.bigdata_sketching.algoritms.Histogram
import org.apache.hadoop.util.bloom.BloomFilter
import org.apache.hadoop.util.hash.Hash
import org.apache.hadoop.util.bloom.Key
import java.io._


case class RFSketch(val candidates : Array[List[Double]],
                    val samples_labels : Array[(Int,Int)], /*id,label*/
                    val num_samples : Int,
                    val num_features : Int,
                    val num_labels : Int,
                    val num_candidates : Int,
                    val p : Double ){

  val bloom_filters = scala.collection.mutable.Buffer[BloomFilter]()
  for(i<-0 until num_sketches){
    bloom_filters += newBloomFilter
  }

  def get_bloom_filter( label : Int ) = bloom_filters(label%num_sketches)

  def write_filter(filename : String ) {
    /*println("store to file")
    val fos = new FileOutputStream(filename);
    val dos = new DataOutputStream(fos);
    filter.write(dos)
    // write histograms
    fos.close()
    dos.close()*/

  }
  // not implemented yet
  def write_histograms(filename : String ) {
  }

  /*def num_features = 784
  def candidates = 10
  def num_samples = 1000

  // false positive in percentage
  def p = 0.01*/

  // real items
  // prediction in worst case
  // the real case differs from this value. This is due to the fact
  // that invalid split candidates are filtered out
  def n = num_samples.toDouble * num_features.toDouble * num_candidates.toDouble * num_labels.toDouble

  def num_sketches = 1 //num_labels

  // lets say we want 10% of the storage
  def m = -n*Math.log(p) / (Math.pow( Math.log(2),2.0)) //(0.1*n).toInt

  def k = m/n * Math.log(2)

  def bloomFilterSize = m.toInt
  def numHashfunctions = if( k.toInt > 0 ) k.toInt else 1

  def newBloomFilter = {
    println("size: "+(m/8/1024/1024/1024)+" gb")
    new BloomFilter( bloomFilterSize, numHashfunctions, Hash.MURMUR_HASH )
  }

}

class RFSketchingPhase(val num_features : Int, val candidates : Int, val num_samples : Int , val num_labels : Int, val p : Double ) {


  // built histograms are scaled to this maximum number of bins
  // representing the split canidates
  val max_bins = candidates

  // not implemented yet
  def read_histograms( in : String ) = {
    //val source = scala.io.Source.fromFile(file)
    //val histograms = new Array[Histogram](num_features)
  }

  def read_sketch( in : String ) = {
    /*val bloom_filter = getBloomFilter
    val fis = new FileInputStream(in);
    val dis = new DataInputStream(fis);

    bloom_filter.readFields(dis)

    fis.close()
    dis.close()

    bloom_filter*/
  }

  def build_sketch( in : String ) = {
    val samples_labels = scala.collection.mutable.Buffer[(Int,Int)]()
    val histograms = new Array[Histogram](num_features)
    val feature_candidates = new Array[List[Double]](num_features)
    for(feature_index<-0 until num_features) {
     histograms(feature_index) = new Histogram(feature_index, max_bins)
    }

    val file=in
    val source = scala.io.Source.fromFile(file)
    var lines = source.getLines

    lines.foreach( x=>{
      val values=x.split(" ")
      val index=values(0).toInt
      val label=values(1).toInt
      val features=values.takeRight(values.size-2)

      samples_labels +=( (index, label) )

      for(i<- 0 until features.size ){
        histograms(i).update(features(i).toDouble )
      }
      if(index%1000==0)println("line "+index)
    })
    println("histograms built")

    val sketch = new RFSketch(feature_candidates, samples_labels.toArray, num_samples, num_features, num_labels, candidates, p )

    println("n "+sketch.n.toLong)
    println("m "+sketch.m.toLong)
    println("size: "+(sketch.m/8/1024/1024/1024)+" gb")
    println("k "+sketch.k.toInt)
    println("numHashfunctions "+sketch.numHashfunctions)


    lines =  scala.io.Source.fromFile(file).getLines
    lines.foreach( x=>{
      val values=x.split(" ")
      val index=values(0).toInt
      val label=values(1).toInt
      val features=values.takeRight(num_features/*values.size-2*/)

      for(f<- 0 until features.size ){
        val candidates = histograms(f).uniform(max_bins)
        feature_candidates(f) = candidates
        candidates.foreach( c => {
          val value = features(f);
          val keyL="key_" + index + "_"+ f +"_"+ c +"_" + label+"_L"
          val keyR="key_" + index + "_"+ f +"_"+ c +"_" + label+"_R"
          if( value.toDouble <= c )
            sketch.get_bloom_filter(label).add(new Key(keyL.getBytes()))
          else
            sketch.get_bloom_filter(label).add(new Key(keyR.getBytes()))
        })
      }

      // insert label info
      val key="key_" + index +"_" + label
      sketch.get_bloom_filter(label).add(new Key(key.getBytes()))

      if(index%1000==0)println("line "+index)
    })

    println("sketch built")

    sketch
  }
}
