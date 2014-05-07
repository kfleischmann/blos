package main.scala.tu_berlin.bigdata_sketching.sketches

import util.Random
import scala.collection.mutable.PriorityQueue

case class Hashfunction(BIG_PRIME :  BigInt, w:Int ) extends Serializable {
  def random_parameter = Math.abs(Random.nextLong())
  val a :BigInt = random_parameter
  val b :BigInt = random_parameter

  def hash( x: Long ) = {
    (a*x+b) % BIG_PRIME % w
  }
}

class CMSketch(delta : Double, epsilon : Double, k : Int ) extends Serializable {
  val BIG_PRIME :BigInt = 9223372036854775783L

  // weights -> space
  val w = Math.ceil( Math.exp(1) / epsilon ).toInt

  // number of hash functions
  val d = Math.ceil( Math.log(1 / delta)).toInt

  var hashfunctions = generate_hashfunctions
  var count : Array[Array[Int]] = null
  var heap : PriorityQueue[(Int, String)] = null
  var top_k : scala.collection.mutable.HashMap[String, (Int, String)] = null

  def alloc = {
    count = Array.ofDim[Int](d, w)
    heap = new PriorityQueue[(Int, String)]()(Ordering.by(estimate))
    top_k = scala.collection.mutable.HashMap[String, (Int, String)]()
  }

  def get_heap = heap
  def size = if(count == null) 0 else d*w
  def estimate(t: (Int,String)) = -get(t._2)
  def get_hashfunctions = hashfunctions
  def set_hashfunctions(h:java.util.ArrayList[Hashfunction]) { hashfunctions = h }
  def update( key : String, increment : Int ) = {
    for( row <- 0 until hashfunctions.size ){
      val col = hashfunctions.get(row).hash(Math.abs(key.hashCode)).toInt
      count(row)(col) += increment
    }
    update_heap(key)
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
    var result = Int.MaxValue
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


  def mergeWith( s : CMSketch ) = {
    for( x <- 0 until d){
      for( y <- 0 until w ) {
        count(x)(y) += s.count(x)(y)
      }//for
    }//for
    // okay i am not sure to be completly correct do find the overall top
    heap.foreach( { x => update_heap(x._2)} )
    s.heap.foreach( { x => update_heap(x._2)} )
  }
}
