package eu.blos.scala.ml.regression

import eu.blos.scala.sketches._
import java.io.{ File,  FileReader}
import eu.blos.scala.inputspace.{Vectors, DataSetIterator, DynamicInputSpace, InputSpaceNormalizer}
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace.Vectors.DoubleVector


/**
 * sketch-based regression models
 * depending on the regression model func a linear or a logistic regression is applied
 */
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
      (predict(x) - y)*x.elements(d)
    }
  }

  class LogisticRegressionModel(model:DoubleVector) extends RegressionModel(model) {
    def predict(x:DoubleVector) : Double = {
      1.0 / ( 1.0 + scala.math.exp(  -(x*model) ))
    }

    def gradient(item:InputSpaceElement, d:Int) : Double = {
      val y = item.vector.elements(0)
      val x = DoubleVector(1.0).append(item.vector.tail)
      (predict(x) - y)*x.elements(d)
    }
  }
  var dimension=2
  var inputDatasetResolution=1
  val numHeavyHitters = 500
  val epsilon = 0.0001
  val delta = 0.01
  val sketch: CMSketch = new CMSketch(epsilon, delta, numHeavyHitters);
  val inputspaceNormalizer = new Rounder(inputDatasetResolution);
  val stepsize =  inputspaceNormalizer.stepSize(dimension)
  val inputspace = new DynamicInputSpace(stepsize);

  def main(args: Array[String]): Unit = {
    val filename = "/home/kay/Dropbox/kay-rep/Uni-Berlin/Masterarbeit/datasets/linear_regression/dataset1"
    val is = new FileReader(new File(filename))

    sketch.alloc

    skeching(sketch,
      new DataSetIterator(is, ","),
      // skip first column (index)
      new TransformFunc() { def apply(x: DoubleVector) = x.tail},
      inputspaceNormalizer
    )
    is.close()

    learning
  }

  def skeching(sketch : CMSketch, dataset : DataSetIterator, t: TransformFunc, normalizer : InputSpaceNormalizer[DoubleVector] ) {
    val i = dataset.iterator
    while( i.hasNext ){
      val vec = normalizer.normalize( t.apply(i.next))
      sketch.update(vec.toString )
      inputspace.update(vec)
    }
  }

  def learning {
    val alpha = 0.1
    var model = Vectors.EmptyDoubleVector(2)+1
    for(x <- Range(1,1000) ){
      model = model - gradient_decent_step( new LinearRegressionModel(model) )*alpha
      println(model)
    }
  }

  def gradient_decent_step(regression : RegressionModel ) : DoubleVector = {
    //val discovery = new SketchDiscoveryEnumeration(sketch, inputspace, inputspaceNormalizer);
    val discovery = new SketchDiscoveryHH(sketch);

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
