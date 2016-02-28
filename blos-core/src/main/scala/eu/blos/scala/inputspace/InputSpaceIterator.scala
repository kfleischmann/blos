package eu.blos.scala.inputspace

import eu.blos.scala.inputspace.Vectors.DoubleVector

case class InputSpaceIterator(min:DoubleVector, max:DoubleVector, stepwide:DoubleVector) extends Iterator[DoubleVector] {
  val current_pos = min

  def hasNext = true
  def next = DoubleVector(2.0)
}
