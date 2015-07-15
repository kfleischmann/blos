package eu.blos.scala.algorithms.sketches

import pl.edu.icm.jlargearrays.FloatLargeArray
import eu.blos.java.algorithms.sketches.{Sketch, HashFunction, DigestHashFunction}

case class ModCMSketch(  var delta: Double,
                      var epsilon: Double
                ) extends Sketch with Serializable {

  def this() = {
    this(1,1/*,1, None*/ )
  }

  var hashfunctions = create_hashfunctions
  val BIG_PRIME : Long = 9223372036854775783L
  def w = Math.ceil(Math.exp(1) /epsilon).toLong
  def d = Math.ceil(Math.log(1 / delta)).toLong

  // sketh data
  var count : FloatLargeArray = null;

  def alloc = {
    count = new FloatLargeArray(w*d, true /*init memory with zeros*/ )
  }

  def size = if(count == null) 0 else d*w
  def estimate(t: (Float,String)) = -get(t._2)
  def get_hashfunctions = hashfunctions
  def getHashfunctions = hashfunctions.toArray( new Array[HashFunction]( hashfunctions.size() ) )
  def array_get(row : Long ,col : Long ) : Float = {
    count.get(row*w+col)
  }

  def array_set(row : Long ,col : Long, value : Float ) {
    count.set(row * w + col, value )
  }

  def update( key : String, increment : Float ) = {
    for( row <- 0 until get_hashfunctions.size ){
      val col = get_hashfunctions.get(row).hash(key).toInt
      array_set(row,col, array_get(row,col)+increment ) //, count(row)(col)
    }
  }

  def +( key : String, increment : Float ) = {
    update(key,increment)
  }

  def get( key : String ) = {
    var result = Float.MaxValue
    for( row <- 0 until get_hashfunctions.size ){
      val col = get_hashfunctions.get(row).hash(key).toLong
      result = Math.min( array_get(row, col), result )
    }
    result
  }

  def create_hashfunctions = {
    val hf = new java.util.ArrayList[HashFunction]()
    for ( x <- 0 until d.toInt ){
      hf.add( new DigestHashFunction(w.toLong, x) )
    }
    hf
  }

  def mergeWith( cms : ModCMSketch ) = {
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
