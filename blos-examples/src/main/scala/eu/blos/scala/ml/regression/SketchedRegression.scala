package eu.blos.scala.ml.regression

import eu.blos.scala.sketches.{DiscoveryStrategy, SketchDiscoveryHH, SketchDiscoveryEnumeration, CMSketch}
import eu.blos.scala.inputspace.Vectors.DoubleVector
import java.io.{InputStreamReader, File, BufferedReader, FileReader}
import eu.blos.scala.inputspace.{Vectors, DataSetIterator, DynamicInputSpace, InputSpaceNormalizer}
import eu.blos.scala.inputspace.normalizer.Rounder


trait TransformFunc {
  def apply(x:DoubleVector) : DoubleVector;
}

trait Model[T] {
  def apply(x:DoubleVector) : T;
}

abstract case class RegressionModel(model:DoubleVector)  extends Model[Double];


class LinearRegressionModel(model:DoubleVector) extends RegressionModel(model) {
  def apply(x:DoubleVector) : Double = {
    x*model
  }
}

class LogisticRegressionModel(model:DoubleVector) extends RegressionModel(model) {
  def apply(x:DoubleVector) : Double = {
    1.0 / ( 1.0 + scala.math.exp(  -(x*model) ))
  }
}


/**
 * sketch-based regression models
 * depending on the regression model func a linear or a logistic regression is applied
 */
object SketchedRegression {
  var inputDatasetResolution=2
  val numHeavyHitters = 500
  val epsilon = 0.00001
  val delta = 0.01
  val sketch: CMSketch = new CMSketch(epsilon, delta, numHeavyHitters);
  val inputspaceNormalizer = new Rounder(inputDatasetResolution);
  val stepsize =  inputspaceNormalizer.stepSize(inputDatasetResolution)
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
    val alpha = 0.5
    var model = Vectors.EmptyDoubleVector(2)+1
    for(x <- Range(1,100) ){
      model = model - gradient_decent_step( new LogisticRegressionModel(model) )*alpha
      println(model)
    }
  }

  def gradient_decent_step(regression : RegressionModel ) : DoubleVector = {
    // discovery = new SketchDiscoveryEnumeration(sketch, inputspace, inputspaceNormalizer);
    val discovery = new SketchDiscoveryHH(sketch);

    var total_freq : Long = 0L
    var gradient = Vectors.EmptyDoubleVector(regression.model.length)*0
    while(discovery.hasNext){
      val item = discovery.next

      // for each dimension
      for( d <- Range(0,regression.model.length)){
        gradient.elements(d) += - ( item.vector.elements(0) - item.count.toDouble * regression.apply( DoubleVector(1.0).append(item.vector.tail))) * item.vector.elements(d)
      }
      total_freq += item.count
    }
    gradient /= total_freq.toDouble
    gradient
  }


}
