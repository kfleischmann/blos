package eu.blos.scala.algorithms.sketches

import pl.edu.icm.jlargearrays.LongLargeArray
import eu.blos.java.algorithms.sketches.{PriorityQueue, Sketch, HashFunction, DigestHashFunction}
import org.apache.commons.lang3.StringUtils

case class HeavyHitters(var maxSize : Int ) extends PriorityQueue[CMEstimate] with Serializable {
  val lnOf2 = scala.math.log(2)
  def log2(x: Double): Double = scala.math.log(x) / lnOf2
  def searchSpaceBestReplacement = math.pow( 2, log2(maxSize)-1 ).toInt+1

  initialize( maxSize, searchSpaceBestReplacement )

  def lessThan(a :CMEstimate, b : CMEstimate ) = a.count>b.count;
  def heapify( key : String ){
    var index = -1;
    for( i <- 1 until getHeapArray.length ){
      val o = getHeapArray.toList(i);
      if( o != null ){
        val e = o.asInstanceOf[CMEstimate];
        if( e.key.equals(key) ){
          index = i
        }
      }
    }

    if(index>0) {
      this.upHeap(index)
    }
  }
}

case class CMEstimate( var count : Long,
                       var key : String );

class CMSketch(  var delta: Double,
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
  var top_k = collection.mutable.HashMap[String, CMEstimate ]();

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
  def getTopK = top_k

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
    val estimate : Long = get(key)

    if(top_k.contains(key)){
      val old_pair = top_k.get(key).get
      old_pair.count = estimate
      heavyHitters.heapify(key);

    } else  {
      // okay we do not know that element
      if(top_k.size < k ) {
        // do we have enough space?

        val new_pair = new CMEstimate(estimate, key)

        heavyHitters.add( new_pair )
        top_k +=( (key, new_pair ) )

      } else {
        // insert new heap is updated automatically

        // only update heap if we benefic from it
        val new_pair = new CMEstimate(estimate, key)
        val old_pair = heavyHitters.tryinsert(new_pair)

        top_k -= old_pair.key
        top_k += ((key, new_pair))
      }

    }
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
