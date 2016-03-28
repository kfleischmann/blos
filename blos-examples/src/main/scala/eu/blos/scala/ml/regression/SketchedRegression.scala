package eu.blos.scala.ml.regression

import eu.blos.scala.inputspace.Vectors._
import eu.blos.scala.sketches.{DiscoveryStrategy, CMSketch, InputSpaceElement}
import eu.blos.scala.inputspace.Vectors.DoubleVector
import eu.blos.scala.inputspace.{InputSpace, Vectors, InputSpaceNormalizer, DataSetIterator}

object SketchedRegression {
  trait TransformFunc {
    def apply(x:DoubleVector) : DoubleVector;
  }

  trait Model[T] {
    def predict(x:DoubleVector) : T;
  }

  abstract case class RegressionModel(model:DoubleVector) extends Model[Double] {
    def gradient(item:InputSpaceElement, d:Int) : Double
  }


  class LinearRegressionModel(model:DoubleVector) extends RegressionModel(model) {
    def predict(x:DoubleVector) : Double = {
      x*model
    }
    def gradient(item:InputSpaceElement, d:Int) : Double = {
      val y = item.vector.elements(0)
      val x = DoubleVector(1.0).append(item.vector.tail)
      (predict(x) - y)*x.elements(d) * item.count
    }
  }

  class LogisticRegressionModel(model:DoubleVector) extends RegressionModel(model) {
    def predict(x:DoubleVector) : Double = {
      1.0 / ( 1.0 + scala.math.exp(  -(x*model) ))
    }

    def gradient(item:InputSpaceElement, d:Int) : Double = {
      val y = item.vector.elements(0)
      val x = DoubleVector(1.0).append(item.vector.tail)
      (predict(x) - y)*x.elements(d) * item.count
    }
  }

  def skeching(sketch : CMSketch, inputspace : InputSpace[DoubleVector], dataset : DataSetIterator, t: TransformFunc, normalizer : InputSpaceNormalizer[DoubleVector] ) {
    val i = dataset.iterator
    while( i.hasNext ){
      val vec = normalizer.normalize( t.apply(i.next))
      sketch.update(vec.toString )
      inputspace.update(vec)
    }
  }

  def gradient_decent_step(regression : RegressionModel, discovery:DiscoveryStrategy ) : DoubleVector = {
    var total_freq : Long = 0L
    var gradient = Vectors.EmptyDoubleVector(regression.model.length)*0.0
    while(discovery.hasNext){
      val item = discovery.next

      // for each dimension
      for( d <- Range(0,regression.model.length)) {
        gradient.elements(d) += (regression.gradient(item, d))
      }
      total_freq += item.count
    }
    gradient /= total_freq.toDouble

    gradient
  }
}
