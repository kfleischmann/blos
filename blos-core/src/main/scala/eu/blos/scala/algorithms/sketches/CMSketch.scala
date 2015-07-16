package eu.blos.scala.algorithms.sketches

import pl.edu.icm.jlargearrays.LongLargeArray
import eu.blos.java.algorithms.sketches.{Sketch, HashFunction, DigestHashFunction}


case class CMSketch(  var delta: Double,
                      var epsilon: Double) extends Sketch with Serializable {

  def this() = {
    this(1,1/*,1, None*/ )
  }

  var hashfunctions = create_hashfunctions
  val BIG_PRIME : Long = 9223372036854775783L

  def w = Math.ceil(Math.exp(1) /epsilon).toLong
  def d = Math.ceil(Math.log(1 / delta)).toLong

  // sketh data
  var count : LongLargeArray = null;

  def alloc = {
    count = new LongLargeArray(w*d, true /*init memory with zeros*/ )
  }

  def size = if(count == null) 0 else d*w
  def estimate(t: (Float,String)) = -get(t._2)
  def get_hashfunctions = hashfunctions
  def getHashfunctions = hashfunctions.toArray( new Array[HashFunction]( hashfunctions.size() ) )
  def array_get(row : Long ,col : Long ) : Long = {
    count.get(row*w+col)
  }

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
}
