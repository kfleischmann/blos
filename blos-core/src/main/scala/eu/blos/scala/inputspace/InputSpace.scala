package eu.blos.scala.inputspace


import eu.blos.scala.inputspace.Vectors.DoubleVector

trait InputSpace[T] extends Iterable[T] {
  def iterator : Iterator[T]
  def update( v : T)
  def getMax() : T;
  def getMin() : T;
  //def getRandom() : T;
  //def getTotalElements() : Long;
}

class InputSpaceIterator(min:DoubleVector, max:DoubleVector, stepwide:DoubleVector) extends Iterator[DoubleVector] {
  var nextVec = DoubleVector(min.elements.clone())
  def hasNext   = nextVec != null
  def next   : DoubleVector   = {
    if(nextVec != null ){
      val result = DoubleVector(nextVec.elements.clone())
      if( !incStepWithFlip( nextVec, 0 ) ){
       nextVec = null;
      }
      result
    } else {
      null
    }
  }
  def incStepWithFlip(v:DoubleVector, d:Int) : Boolean = {
    if(d > v.length-1 ) {
      false
    } else {
      // if not enough space available?
      if( v.elements(d) + stepwide.elements(d) > max.elements(d) ){
        v.elements(d) = min.elements(d)
        incStepWithFlip(v, d+1 )
      } else {
        v.elements(d) += stepwide.elements(d)
        true
      }
    }
  }
}

class StaticInputSpace(min:DoubleVector, max:DoubleVector, step:DoubleVector) extends InputSpace[DoubleVector] {
  def update(v:DoubleVector) = {}
  def getMin = min
  def getMax = max
  def iterator = new InputSpaceIterator(min,max,step)
}

class DynamicInputSpace(step:DoubleVector) extends InputSpace[DoubleVector] {
  var min : Option[DoubleVector] = None
  var max : Option[DoubleVector] = None
  def getMin = min.get
  def getMax = max.get
  def update (v:DoubleVector) = {
    min match {
      case None => min = Some(DoubleVector(v.elements.clone()))
      case Some(s) => {
        var a = 0
        val d = s.elements.length
        while (a < d) {
          s.elements(a) = Math.min(s.elements(a), v.elements(a))
          a = a + 1
        }
      }
    }
    max match {
      case None => max = Some(DoubleVector(v.elements.clone()))
      case Some(s) => {
        var a = 0
        val d = s.elements.length
        while (a < d) {
          s.elements(a) = Math.max(s.elements(a), v.elements(a))
          a = a + 1
        }
      }
    }
  }
  def iterator = new InputSpaceIterator(min.get,max.get,step)
}