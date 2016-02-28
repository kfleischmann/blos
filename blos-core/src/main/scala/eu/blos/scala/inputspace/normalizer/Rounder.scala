package eu.blos.scala.inputspace.normalizer

import eu.blos.scala.inputspace.{InputSpaceNormalizer }
import eu.blos.scala.inputspace.Vectors.DoubleVector

class Rounder() extends InputSpaceNormalizer[DoubleVector]{
  val max : DoubleVector = null
  val min : DoubleVector = null

  def getMax() : DoubleVector = max
  def getMin() : DoubleVector = min

  def normalize(vec:DoubleVector): DoubleVector = {
    vec
  }

  def update(vec:DoubleVector) {
  }

  def iterator() : Iterator[DoubleVector] = {
    null
  }

  def getRandom() : DoubleVector = {
    DoubleVector()
  }
  def getTotalElements() : Long = {
    0
  }

}
