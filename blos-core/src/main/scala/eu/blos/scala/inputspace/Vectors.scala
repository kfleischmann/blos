package eu.blos.scala.inputspace

object Vectors {
  case class DoubleVector(v: Array[Double]) extends InputSpaceVector[Double] {
    type Self = DoubleVector
    def *(x: Double) = rep(k => k * x)
    def +(x: Double) = rep(k => k + x)
    def /(x: Double) = rep(k => k / x)
    def -(x: Double) = rep(k => k - x)
    def == (x: DoubleVector ) = {
      v.sameElements( x.elements )
    }
    def < (x:DoubleVector) : Boolean = {
      var b = true
      for( k <- 0 until dimension ) {
        if ( !(elements(k) <= x.elements(k)) ){
          b = false
        }
      }
      b
    }
    def > (x:DoubleVector) : Boolean = {
      var b = true
      for( k <- 0 until dimension ) {
        if ( !(elements(k) >= x.elements(k)) ){
          b = false
        }
      }
      b
    }

    def dimension = v.length
    def elements = v

    private def rep(f: Double => Double): Self = {
      val nv = new Array[Double](v.length)
      var i = 0
      while (i < nv.length) {
        nv(i) = f(v(i))
        i += 1
      }
      DoubleVector(nv)
    }

    def apply(i: Int) = v(i)

    def length = v.length
  }

  implicit def doubleArrayToVector(arr: Array[Double]) = new DoubleVector(arr)

  def DoubleVector(d: Double*) = new DoubleVector(d.toArray)

  // find component wise minimum
  def MinSpaceVector(vec:List[DoubleVector]) : DoubleVector = {
    val res = DoubleVector( vec.head.elements.clone().map( x => java.lang.Double.POSITIVE_INFINITY ))
    for( k <- 0 until vec.head.dimension ) {
      var i=0
      while( i < vec.length ){
        res.elements(k) = Math.min( res(k), vec(i).elements(k) )
        i += 1
      } //while
    } //for
    res
  }
  // find component wise maxmimum
  def MaxSpaceVector(vec:List[DoubleVector]) : DoubleVector = {
    val res = DoubleVector( vec.head.elements.clone().map( x => java.lang.Double.NEGATIVE_INFINITY ))
    for( k <- 0 until vec.head.dimension ) {
      var i=0
      while( i < vec.length ){
        res.elements(k) = Math.max( res(k), vec(i).elements(k) )
        i += 1
      } //while
    } //for
    res
  }


}