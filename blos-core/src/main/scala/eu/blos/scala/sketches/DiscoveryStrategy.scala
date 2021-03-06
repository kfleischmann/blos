package eu.blos.scala.sketches

import eu.blos.scala.inputspace.{InputSpaceNormalizer, InputSpace}
import eu.blos.scala.inputspace.Vectors.DoubleVector

case class InputSpaceElement(count:Long, vector:DoubleVector);

trait DiscoveryStrategy extends Iterable[InputSpaceElement] {
}

class DiscoveryStrategyHH(cms:CMSketch) extends DiscoveryStrategy {
  def iterator = {
    new SketchDiscoveryHHIterator(cms)
  }
}

class DiscoveryStrategyEnumeration(sketch:CMSketch, inputspace : InputSpace[DoubleVector], inputspaceNormalizer : InputSpaceNormalizer[DoubleVector]) extends DiscoveryStrategy {
  def iterator = {
    new SketchDiscoveryEnumerationIterator(sketch, inputspace,inputspaceNormalizer )
  }
}


class SketchDiscoveryEnumerationIterator(sketch:CMSketch, inputspace : InputSpace[DoubleVector], inputspaceNormalizer : InputSpaceNormalizer[DoubleVector] ) extends Iterator[InputSpaceElement] {
  val iterator = inputspace.iterator
  def hasNext = iterator.hasNext
  def next : InputSpaceElement = {
    if(hasNext) {
      val v = inputspaceNormalizer.normalize(iterator.next)
      new InputSpaceElement( sketch.get(v.toString), v )
    } else {
      null
    }
  }
}

class SketchDiscoveryHHIterator(cms:CMSketch) extends Iterator[InputSpaceElement] {
  val hh = cms.getHeavyHitters
  val numHH = hh.getSize
  var posHH = 1
  def hasNext = posHH < numHH
  def next : InputSpaceElement = {
    if (hasNext) {
      val topK : CMEstimate = hh.getHeapArray()(posHH).asInstanceOf[CMEstimate]
      if (topK != null) {
        posHH = posHH + 1
        new InputSpaceElement( topK.count, DoubleVector( topK.key.replaceAll("[^-0-9,.E]", "").split(",") map( x => x.toDouble) ))
      } else {
        null
      }
    } else {
      null
    }
  }
}