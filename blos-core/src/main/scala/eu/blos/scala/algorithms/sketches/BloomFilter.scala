package eu.blos.scala.algorithms.sketches

/**
 * Naive implementation of the Bloom Filter to manage the membership to very large set of elements
 * This code is provided solely for the purpose of illustrate concepts introduced or described
 * in some of the  of the post in the blog
 *
 * @author Patrick Nicolas
 * @date October 2, 2013
 * @see http://patricknicolas.blogspot.com
 */
import java.security._
import java.math.BigInteger
import eu.blos.java.algorithms.sketches.HashFunction


/**
 * Basic and simple implementation of the Bloom filter
 * @param initialCapacity  capacity of the Bloom filter (size of the bitvector)
 * @param numHashs number of hash function used to associated an element to a position in the bit vector
 * @param algorithm hashing algorithms used to compute the position in the bit vector (MD5, SHA-1,...)
 */
class BloomFilter(private val initialCapacity: Int,
                        private val numHashs: Int,
                        private val algorithm: String) extends Serializable {

  require( initialCapacity > 4, "Bloom filter capacity is too small")
  require( numHashs > 0, "Bloom filter has no hash function")
  require( algorithm != null, "Bloom filter has bit position algorithm")
  require(initialCapacity > numHashs, "Bloom filter capacity exceeds number of hash functions")

  private[this] val set = new Array[Byte](initialCapacity)
  private[this] var numElements = 0
  private[this] val digest = {
    try { MessageDigest.getInstance(algorithm) }
    catch {case e: NoSuchAlgorithmException => null }
  }


  def getHashfunctions() : Array[HashFunction] = {
    null
  }

  /**
   * Adds an array of elements to this filter.
   */
  def add(elements: Array[Any]): Unit = {
    require( elements != null && elements.size > 0, "Cannot add undefined array of element to the Bloom filter")

    if( digest != null) {
      elements.foreach( getSet(_).foreach( set(_) = 1) )
      numElements += elements.size
    }
  }

  /**
   * Add a single element to the filter
   * @param el element to add to the filter
   */
  @inline
  def add(el: Any): Unit = { require(el != null, "Cannot add undefined item into Bloom Filter"); this.add(Array[Any](el))  }

  /**
   * Sketching whether an element belongs to the set.
   * @param el element for which the membership is evaluated
   * @return true if element is not null, a digest exists and the element is a member, false otherwise
   */
  @inline
  def contains(el: Any): Boolean = if( digest != null && el != null) !getSet(el).exists( set(_) != 1) else false


  import BloomFilter._
  private[this] def hash(value: Int) : Int = {
    digest.reset
    digest.update(value)
    Math.abs(new BigInteger(1, digest.digest).intValue) % (set.size -1)
  }

  private[this] def getSet(el: Any): Array[Int] = {
    val newSet = new Array[Int](numHashs)
    newSet.update(0, hash(el.hashCode))
    getSet(newSet, 1)
    newSet
  }

  @scala.annotation.tailrec
  private[this] def getSet(intElements: Array[Int], index: Int) : Boolean = {
    if( index >= intElements.size)
      true
    else {
      intElements.update(index, hash(intElements(index-1)))
      getSet(intElements, index+1)
    }
  }
}

/**
 * Companion object used to implement the integer to byte conversion
 */
object BloomFilter extends Serializable  {
  implicit def Int2Bytes(value: Int) : Array[Byte] = {
    val bytes = new Array[Byte](4)
    bytes.map( x => { val offset = (bytes.size -1 - bytes.indexOf(x)) <<3; ((value >>> offset) & 0xFF).asInstanceOf[Byte]  })
    bytes
  }

  def main(args: Array[String]) {

    val filter = new BloomFilter(100, 5, "SHA")

    val newValues = Array[Any](57, 97, 91, 23, 67,33)

    filter.add(newValues)

    println( filter.contains(22) )
    println( filter.contains(23) )
  }
}