package main.scala.tu_berlin.bigdata_sketching.examples.random_forest.local

import main.scala.tu_berlin.bigdata_sketching.algoritms.sketches._
import main.scala.tu_berlin.bigdata_sketching.algoritms.Histogram
import org.apache.hadoop.util.bloom.BloomFilter
import org.apache.hadoop.util.hash.Hash
import org.apache.hadoop.util.bloom.Key
import java.io._


object SketchingPhase {
  def Gbits = 1024*1024*1024.0*8

  // 10 GB
  def num_features = 784
  def candidates = 5
  def num_samples = 1000

  // real items
  def n = num_samples.toDouble*num_features.toDouble*candidates.toDouble

  // 1% false positive
  def p = 0.01

  // lets say we want 10% of the storage
  def m = -n*Math.log(p) / (Math.pow( Math.log(2),2.0)) //(0.1*n).toInt

  def k = m/n * Math.log(2)

  def bloomFilterSize = m.toInt
  def numHashfunctions = k.toInt

  val max_bins = candidates

  def getBloomFilter = {
    new BloomFilter( bloomFilterSize, numHashfunctions, Hash.MURMUR_HASH )
  }


  def main(args: Array[String]) {
    build_sketch("/home/kay/normalized_very_small.txt", "/home/kay/bloomfilter_very_small")
    read_sketch("/home/kay/bloomfilter_very_small")
  }

  def read_sketch( in : String ) {
    val bloom_filter = getBloomFilter
    val fis = new FileInputStream(in);
    val dis = new DataInputStream(fis);

    bloom_filter.readFields(dis)

    fis.close()
    dis.close()
  }

  def build_sketch( in : String, out : String) = {
    println("n "+n.toLong)
    println("m "+m.toLong)
    println("size: "+(m/8/1024/1024/1024)+" gb")
    println("k "+k.toInt)
    println("numHashfunctions "+numHashfunctions)

    System.exit(0)

    val bloom_filter = getBloomFilter

    val histograms = new Array[Histogram](num_features)
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

      for(i<- 0 until features.size ){
        histograms(i).update(features(i).toDouble )
      }
      if(index%1000==0)println("line "+index)
    })

    println("histograms built")

    lines =  scala.io.Source.fromFile(file).getLines
    lines.foreach( x=>{
      val values=x.split(" ")
      val index=values(0).toInt
      val label=values(1).toInt
      val features=values.takeRight(num_features/*values.size-2*/)

      for(f<- 0 until features.size ){
        val candidates = histograms(f).uniform(max_bins)
        candidates.foreach( c => {
          val value = features(f);
          val key="key_" + index + "_"+ f +"_"+ c+"_" + label
          if( value.toDouble <= c )
            bloom_filter.add(new Key(key.getBytes()))
        })
      }
      if(index%1000==0)println("line "+index)
    })

    println("sketch built")
    println("store to file")

    val fos = new FileOutputStream(out);
    val dos = new DataOutputStream(fos);

    bloom_filter.write(dos)

    println("measure quality")

    var errors = 0
    var total=0

    lines =  scala.io.Source.fromFile(file).getLines
    lines.foreach( x=>{
      val values=x.split(" ")
      val index=values(0).toInt
      val label=values(1).toInt
      val features=values.takeRight( num_features /*values.size-2*/)
      for(f<- 0 until features.size ){
        val candidates = histograms(f).uniform(max_bins)
        candidates.foreach( c => {
          total+=1
          val value = features(f);
          val key="key_" + index + "_"+ f +"_"+ c+"_" + label
          if( value.toDouble <= c ) {
            if( !bloom_filter.membershipTest(new Key(key.getBytes())) ){
              errors+=1
              //println(key)
            }
          } else {
            if( bloom_filter.membershipTest(new Key(key.getBytes())) ){
              errors+=1
              //println(key)
            }
          }
        })
      }
      if(index%1000==0){
        println("line "+index)
        println("accuracy: "+ (1.0-(errors.toDouble/total.toDouble)))
      }
    })
    println("------------------------")
    println("total: "+total)
    println("errors: "+errors)
    println("accuracy: "+ (1.0-(errors.toDouble/total.toDouble)))

    fos.close()
    dos.close()
  }
}
