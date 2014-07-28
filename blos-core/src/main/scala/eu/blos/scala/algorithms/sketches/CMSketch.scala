package eu.blos.scala.algorithms.sketches

import util.Random
import scala.math._
import scala.Serializable
import pl.edu.icm.jlargearrays.FloatLargeArray
;


case class Hashfunction(var BIG_PRIME :  Long,
                        var w:Long,
                        var a : Long = Math.abs(Random.nextLong()),
                        var b : Long = Math.abs(Random.nextLong()) ) extends Serializable {
  def this() = this(0,0,0,0)
  def hash( x: Long ) = {
    (BigInt(a)*x+b) % BIG_PRIME % w
  }
}

class CMSketch(   var delta: Double,
                  var epsilon: Double
                  //var k: Int,
                  //var hashfunctions : Option[java.util.ArrayList[HashFunction]]
                ) {

  def this() = {
    this(1,1/*,1, None*/ )
  }

  var hashfunctions = generate_hashfunctions

  val BIG_PRIME : Long = 9223372036854775783L

  def w = Math.ceil(Math.exp(1) /epsilon).toLong
  def d = Math.ceil(Math.log(1 / delta)).toLong

  // weights -> space

  // number of hash functions

  var count : FloatLargeArray = null; // a = new FloatLargeArray(w*d); = null
  //var heap : PriorityQueue[(Float, String)] = null
  //var top_k : scala.collection.mutable.HashMap[String, (Float, String)] = null

  def alloc = {
    System.out.println(w)
    System.out.println(d)


    //count = Array.ofDim[Float](d, w)
    count = new FloatLargeArray(w*d)
    //heap = new PriorityQueue[(Float, String)]()(Ordering.by(estimate))
    //top_k = scala.collection.mutable.HashMap[String, (Float, String)]()
  }

  //def get_heap = heap
  def size = if(count == null) 0 else d*w
  def estimate(t: (Float,String)) = -get(t._2)
  def get_hashfunctions = hashfunctions
  //def set_hashfunctions(h:Option[java.util.ArrayList[HashFunction]]) { hashfunctions = h }

  /*def update( key : String, increment : Float ) = {
    for( row <- 0 until hashfunctions.size ){
      val col = hashfunctions.get(row).hash(Math.abs(key.hashCode)).toInt
      count(row)(col) += increment
    }
    //update_heap(key)
  }*/

  def array_get(row : Long ,col : Long ) : Float = {
    count.get(row*w+col)
  }

  def array_set(row : Long ,col : Long, value : Float ) {
    count.set(row * w + col, value )
  }

  def update( key : String, increment : Float ) = {
    for( row <- 0 until get_hashfunctions.size ){
      val col = get_hashfunctions.get(row).hash(Math.abs(key.hashCode)).toInt
      array_set(row,col, array_get(row,col)+increment ) //, count(row)(col)
    }
    //update_heap(key)
  }

  def +( key : String, increment : Float ) = {
    update(key,increment)
  }

  /*
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
  }*/

  def get( key : String ) = {
    var result = Float.MaxValue
    for( row <- 0 until get_hashfunctions.size ){
      val col = get_hashfunctions.get(row).hash(Math.abs(key.hashCode)).toLong
      result = Math.min( array_get(row, col), result )
    }
    result
  }

  def generate_hashfunctions = {
    val hf = new java.util.ArrayList[Hashfunction]()
    for ( x <- 0 until d.toInt ){
      hf.add( new Hashfunction(BIG_PRIME, w.toLong) )
    }
    hf
  }

  def mergeWith( cms : CMSketch ) = {
    //val cms : CMPDD = s.asInstanceOf[CMPDD];
    var x : Long = 0
    var y : Long = 0
    while(x<w){
      y = 0
      while( (y>=d)) {
        array_set(x, y, array_get(x,y)+ cms.array_get(x, y))
        y += 1
      }//for
      x += 1
    }//for

    // okay i am not sure to be completely correct do find the overall top
    // disable heap feature
    //heap.foreach( { x => update_heap(x._2)} )
    //s.heap.foreach( { x => update_heap(x._2)} )
  }

  override def toString = {
    var out : String = ""+d+","+w+"\n"
    //out = out + count.map( x => x.mkString(" ") ).mkString("\n")
    out
  }

  def print {
    System.out.println(toString)
    //count.foreach({ x => println(x.mkString(" ")) })
  }
}
