package eu.blos.scala.sketches

import eu.blos.scala.inputspace.InputSpace
import eu.blos.scala.inputspace.Vectors.DoubleVector

case class InputSpaceElement(count:Long, v:DoubleVector);

trait DiscoveryStrategy extends Iterator[InputSpaceElement] {
}

class SketchDiscoveryEnumeration(cms:CMSketch, inputspace : InputSpace[DoubleVector] ) extends DiscoveryStrategy {
  def hasNext = false
  def next : InputSpaceElement = {
    null
  }
}

class SketchDiscoveryHH(cms:CMSketch) extends DiscoveryStrategy {
  val hh = cms.getHeavyHitters
  val numHH = hh.getHeapArray.length
  val posHH = 1
  def hasNext = posHH < numHH
  def next : InputSpaceElement = {
    if (hasNext) {
      val topK : CMEstimate = hh.getHeapArray()(1).asInstanceOf[CMEstimate]
      /*
      if (toptopK != null) {
        freq = topK.count();
        val vec = DoubleVector( topK.key.replaceAll("[^-0-9,.E]", "").split(",") )
        posHH++
        vec
      }g
      */
      new InputSpaceElement( topK.count, DoubleVector( topK.key.replaceAll("[^-0-9,.E]", "").split(",") map( x => x.toDouble) ))
    } else {
      null
    }
  }
}