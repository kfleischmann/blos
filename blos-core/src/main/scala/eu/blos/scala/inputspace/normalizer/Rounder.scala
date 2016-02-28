package eu.blos.scala.inputspace.normalizer

import eu.blos.scala.inputspace.{InputSpaceIterator, InputSpaceNormalizer}
import eu.blos.scala.inputspace.Vectors.DoubleVector

class Rounder(precision:Double, stepWidth  : Double) extends InputSpaceNormalizer[DoubleVector]{
  val max : DoubleVector = null
  val min : DoubleVector = null

  def getMax() : DoubleVector = max
  def getMin() : DoubleVector = min

  def normalize(vec:DoubleVector): DoubleVector = {
    DoubleVector( vec.elements.clone() map { x => round(x, precision) } )
  }

  def update(vec:DoubleVector) {
  }

  def iterator() : Iterator[DoubleVector] = {
    new InputSpaceIterator(min, max, stepWidth)
  }

  def getRandom() : DoubleVector = {
    null
  }
  def getTotalElements() : Long = {
    0
  }

  def round (value:Double, precision:Double) ={
    val scale = Math.pow(10, precision);
    Math.round(value * scale) / scale;
  }
}
