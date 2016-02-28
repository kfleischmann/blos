package eu.blos.scala.inputspace

import eu.blos.scala.inputspace.Vectors.DoubleVector

case class InputSpaceIterator(min:DoubleVector, max:DoubleVector, stepwidth:Double) extends Iterator[DoubleVector] {
  var currentPos = min

  def hasNext = true
  def next = {
    null;
    //while(currentPos .< max ){
    //
    //}
  }
}
