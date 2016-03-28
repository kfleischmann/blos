package eu.blos.scala.ml.regression

import eu.blos.scala.sketches.{SketchDiscoveryHH, SketchDiscoveryEnumeration, CMSketch}
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

abstract class RegressionModel(model:DoubleVector)  extends Model[Double];


class LinearRegressionModel(model:DoubleVector) extends RegressionModel(model) {
  def apply(x:DoubleVector) : Double = {
    x*model
  }
}

class LogisticRegressionModel(model:DoubleVector) extends RegressionModel(model) {
  def apply(x:DoubleVector) : Double = {
    val scalar = -(x*model);
    1.0 / ( 1.0 + scala.math.exp( scalar))
  }
}


/**
 * sketch-based regression models
 * depending on the regression model func a linear or a logistic regression is applied
 */
object SketchedRegression {
  var inputDatasetResolution=2
  val numHeavyHitters = 100
  val epsilon = 0.0001
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
    for(x <- Range(1,10000) ){
      model = model - gradient_decent_step(model)*alpha
      println(model)
    }
  }

  def h_theta(Xi:DoubleVector, model: DoubleVector): Double = {
    return (1.0 / (1.0 + Math.exp(-(Xi * model))))
  }

  def gradient_decent_step(model:DoubleVector) : DoubleVector = {
    val discovery = new SketchDiscoveryEnumeration(sketch, inputspace, inputspaceNormalizer);
    //val discovery = new SketchDiscoveryHH(sketch);
    var sum = 0.0
    var total_freq : Long = 0L
    var gradient = Vectors.EmptyDoubleVector(model.length)
    while(discovery.hasNext){
      val item = discovery.next
      // for each dimension
      for( d <- Range(0,model.length)){
        gradient.elements(d) += - ( item.vector.elements(0) - item.count * h_theta( DoubleVector(1.0).append(item.vector.tail), model)) * item.vector.elements(d)
      }

      total_freq += item.count
    }
    gradient /= total_freq
    gradient
  }


}
