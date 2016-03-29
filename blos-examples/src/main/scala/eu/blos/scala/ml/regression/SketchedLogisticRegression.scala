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
object SketchedLogisticRegression {

  import SketchedRegression._

  var numIterations=1000
  var dimension=2
  var inputDatasetResolution=2
  val numHeavyHitters = 500
  val epsilon = 0.001
  val delta = 0.5
  val alpha = 0.5
  val sketch: CMSketch = new CMSketch(delta,epsilon, numHeavyHitters);
  val inputspaceNormalizer = new Rounder(inputDatasetResolution);
  val stepsize =  inputspaceNormalizer.stepSize(dimension)
  val inputspace = new DynamicInputSpace(stepsize);

  def main(args: Array[String]): Unit = {
    val filename = "/home/kay/Dropbox/kay-rep/Uni-Berlin/Masterarbeit/dyatasets/logistic_regression/dataset6_8_1_1k"
    val is = new FileReader(new File(filename))

    sketch.alloc

    skeching(sketch,
      inputspace,
      new DataSetIterator(is, ","),
      // skip first column (index)
      new TransformFunc() { def apply(x: DoubleVector) = x.tail},
      inputspaceNormalizer
    )
    is.close()

    learning
  }

  def learning {
    var model = Vectors.EmptyDoubleVector(2)+1
    for(x <- Range(0,numIterations) ){
      //val discovery = new SketchDiscoveryEnumerationIterator(sketch, inputspace, inputspaceNormalizer);
      val discovery = new SketchDiscoveryHHIterator(sketch);

      model = model - gradient_decent_step( new LogisticRegressionModel(model), discovery )*alpha

      println(model)
    }
  }
}
