package eu.blos.scala.inputspace.normalizer

import eu.blos.scala.inputspace.{StaticInputSpace, InputSpace, InputSpaceNormalizer}
import eu.blos.scala.inputspace.Vectors.DoubleVector

class Rounder(precision:Double ) extends InputSpaceNormalizer[DoubleVector]{
  def normalize(vec:DoubleVector): DoubleVector = {
    DoubleVector( vec.elements.clone() map { x => round(x, precision) } )
  }

  def stepSize(dim:Int) : DoubleVector = {
    DoubleVector( Array.fill[Double](dim)(0.0) )+Math.pow(10, -precision)
  }

  private def round (value:Double, precision:Double) ={
    val scale = Math.pow(10, precision);
    Math.round(value * scale) / scale;
  }
}
