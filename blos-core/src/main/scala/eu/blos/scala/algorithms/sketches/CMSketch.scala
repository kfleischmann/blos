package eu.blos.scala.algorithms.sketches

import util.Random
import scala.collection.mutable.PriorityQueue
import scala.math._
import java.io.{DataInput, DataOutput}
import eu.blos.java.api.common.Sketch;


case class Hashfunction(var BIG_PRIME :  BigInt,
                        var w:Int,
                        var a : BigInt = Math.abs(Random.nextLong()),
                        var b : BigInt = Math.abs(Random.nextLong()) ) extends Serializable {
  def hash( x: Long ) = {
    (a*x+b) % BIG_PRIME % w
  }
}

class CMSketch extends Sketch[CMSketch] {
  var delta : Double = 0.0
  var epsilon : Double = 0.0
  var k : Int = 0

  def this( delta: Double, epsilon : Double, k : Int ) {
    this()
    this.delta = delta
    this.epsilon = epsilon
    this.k = k

    this w = Math.ceil( Math.exp(1) / epsilon ).toInt

    // number of hash functions
    this.d = Math.ceil( Math.log(1 / delta)).toInt

    hashfunctions = generate_hashfunctions
  }

  def write( dataOutput : DataOutput ) {
    dataOutput.writeDouble(delta)
    dataOutput.writeDouble(epsilon)
    dataOutput.writeInt(k)
    dataOutput.writeInt(w)
    dataOutput.writeInt(d)

    for( x <- 0 until d){
      for( y <- 0 until w ) {
        dataOutput.writeFloat( count(x)(y) )
      }//for
    }//for

    for ( x <- 0 until d ){
      dataOutput.writeLong( hashfunctions.get(x).a.toLong )
      dataOutput.writeLong( hashfunctions.get(x).b.toLong )
    }
  }

  def read( dataInput : DataInput ) {
    delta = dataInput.readDouble();
    epsilon = dataInput.readDouble();
    k=dataInput.readInt();
    w=dataInput.readInt()
    d=dataInput.readInt()

    alloc

    for( x <- 0 until d){
      for( y <- 0 until w ) {
        count(x)(y) = dataInput.readFloat()
      }//for
    }//for

    hashfunctions = new java.util.ArrayList[Hashfunction]()
    for ( x <- 0 until d ){
      val a = dataInput.readLong()
      val b = dataInput.readLong()
      hashfunctions.add( new Hashfunction(BIG_PRIME, w, a, b ) )
    }
  }

  val BIG_PRIME : Long = 9223372036854775783L

  // weights -> space
  var w : Int = 0

  // number of hash functions
  var d : Int = 0

  var hashfunctions : java.util.ArrayList[Hashfunction] = null
  var count : Array[Array[Float]] = null
  var heap : PriorityQueue[(Float, String)] = null
  var top_k : scala.collection.mutable.HashMap[String, (Float, String)] = null

  def alloc = {
    count = Array.ofDim[Float](d, w)
    heap = new PriorityQueue[(Float, String)]()(Ordering.by(estimate))
    top_k = scala.collection.mutable.HashMap[String, (Float, String)]()
  }

  def get_heap = heap
  def size = if(count == null) 0 else d*w
  def estimate(t: (Float,String)) = -get(t._2)
  def get_hashfunctions = hashfunctions
  def set_hashfunctions(h:java.util.ArrayList[Hashfunction]) { hashfunctions = h }
  /*def update( key : String, increment : Float ) = {
    for( row <- 0 until hashfunctions.size ){
      val col = hashfunctions.get(row).hash(Math.abs(key.hashCode)).toInt
      count(row)(col) += increment
    }
    //update_heap(key)
  }*/

  def update( key : String, increment : Float ) = {
    for( row <- 0 until hashfunctions.size ){
      val col = hashfunctions.get(row).hash(Math.abs(key.hashCode)).toInt
      count(row)(col) += increment //, count(row)(col)
    }
    //update_heap(key)
  }


  def +( key : String, increment : Float ) = {
    update(key,increment)
  }

  def update_heap( key : String ) = {
    val estimate = get(key)

    // heap empty or the updated value better than the head one
    if (heap.isEmpty || estimate >= heap.head._1) {

      // key exists?
      if ( top_k.exists( {x => x._1 == key})) {
        top_k(key) = (estimate, key)

        // shoudn't i update the heap as well?
        // maybe not, because i only care about the k top items
      } else {
        if (top_k.size < k) {

          heap.enqueue( (estimate, key) )
          top_k(key) = (estimate, key)
        } else {
          val new_pair = (estimate, key)
          val old_pair = heap.dequeue()

          heap.enqueue(new_pair)
          top_k(key) = new_pair
        }
      }
    }
  }

  def get( key : String ) = {
    var result = Float.MaxValue
    for( row <- 0 until hashfunctions.size ){
      val col = hashfunctions.get(row).hash(Math.abs(key.hashCode)).toInt
      result = Math.min( count(row)(col), result )
    }
    result
  }

  def generate_hashfunctions = {
    val hf = new java.util.ArrayList[Hashfunction]()

    for ( x <- 0 until d ){
      hf.add( new Hashfunction(BIG_PRIME, w.toInt) )
    }
    hf
  }

  def mergeWith( cms : CMSketch ) = {
    //val cms : CMSketch = s.asInstanceOf[CMSketch];
    for( x <- 0 until d){
      for( y <- 0 until w ) {
        count(x)(y) += cms.count(x)(y)
      }//for
    }//for

    // okay i am not sure to be completly correct do find the overall top
    // disable heap feature
    //heap.foreach( { x => update_heap(x._2)} )
    //s.heap.foreach( { x => update_heap(x._2)} )
  }

  override def toString = {
    var out : String = ""+d+","+w+"\n"
    out = out + count.map( x => x.mkString(" ") ).mkString("\n")
    out
  }

  def print {
    System.out.println(toString)
    //count.foreach({ x => println(x.mkString(" ")) })
  }

  def clone_mask : CMSketch = {
    val s = new CMSketch ( delta, epsilon, k  )
    s.set_hashfunctions( get_hashfunctions )
    s
  }
}
