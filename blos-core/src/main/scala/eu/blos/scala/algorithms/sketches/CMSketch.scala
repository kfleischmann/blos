package eu.blos.scala.algorithms.sketches

import pl.edu.icm.jlargearrays.LongLargeArray
import eu.blos.java.algorithms.sketches.{PriorityQueue, Sketch, HashFunction, DigestHashFunction}
import org.apache.commons.lang3.StringUtils

class HeavyHitters(var maxSize : Int ) extends PriorityQueue[(Long,String)] with Serializable {
  initialize( maxSize )
  def lessThan(a : (Long, String), b : (Long, String) ) = a._1>b._1;
}

case class CMSketch(  var delta: Double,
                      var epsilon: Double,
                      var k : Integer )  extends Sketch with Serializable {

  def this() = {
    this(1,1,0/*,1, None*/ )
  }

  def this(delta: Double, epsilon: Double ) = {
    this(delta, epsilon,0/*,1, None*/ )
  }

  var hashfunctions = create_hashfunctions
  val BIG_PRIME : Long = 9223372036854775783L

  def w = Math.ceil(Math.exp(1) /epsilon).toLong
  def d = Math.ceil(Math.log(1 / delta)).toLong


  var heavyHitters = new HeavyHitters(k);

  // sketh data
  var count : LongLargeArray = null;

  def alloc = {
    count = new LongLargeArray(w*d, true /*init memory with zeros*/ )
  }

  def size = if(count == null) 0 else d*w*8.0 // Bytes
  def alloc_size = d*w*8.0 // Bytes
  def estimate(t: (Float,String)) = -get(t._2)
  def get_hashfunctions = hashfunctions
  def getHashfunctions = hashfunctions.toArray( new Array[HashFunction]( hashfunctions.size() ) )
  def array_get(row : Long ,col : Long ) : Long = {
    count.get(row*w+col)
  }
  def getHeavyHitters = heavyHitters

  def totalSumPerHash : Long = {
    var total_sum = 0L
    for( col <- 0L until w ) {
      total_sum += count.get(col);
    }
    total_sum
  }

  def beforeHash( key : String ) = key

  def array_set(row : Long ,col : Long, value : Long ) {
    count.set(row * w + col, value )
  }

  def update( key : String, increment : Long ) {
    for( row <- 0 until get_hashfunctions.size ){
      val col = get_hashfunctions.get(row).hash( beforeHash( key ) )
      array_set(row,col, array_get(row,col)+increment ) //, count(row)(col)
    }
  }

  def update( key : String ) {
    update( key, 1L )
    update_heap( key );
  }

  def update_heap(key : String ){
    heavyHitters.insertWithOverflow( (get(key), key ) )

  }

  def +( key : String, increment : Long ) = {
    update(key,increment)
  }

  def get( key : String ) : Long = {
    var result = Long.MaxValue
    for( row <- 0 until get_hashfunctions.size ){
      val col = get_hashfunctions.get(row).hash(beforeHash(key))
      result = Math.min( array_get(row, col), result )
    }
    result
  }

  def create_hashfunctions = {
    val hf = new java.util.ArrayList[HashFunction]()
    for ( x <- 0 until d.toInt ){
      hf.add( new DigestHashFunction(w, x) )
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
  }

  override def toString = {
    ""+d+","+w+"\n"
  }

  def print {
    System.out.println(toString)
  }

  def display {
    var result = Long.MaxValue
    for( row <- 0 until get_hashfunctions.size ){
      for( col <- 0L until w ) {
        result = array_get(row, col)
        System.out.print( StringUtils.leftPad( ""+result, 3, " " )+" " );
      }
      System.out.println();
    }
  }
}
