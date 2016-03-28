package eu.blos.scala.ml.regression

import eu.blos.scala.sketches._
import java.io.{ File,  FileReader}
import eu.blos.scala.inputspace.{Vectors, DataSetIterator, DynamicInputSpace, InputSpaceNormalizer}
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace.Vectors.DoubleVector


/**
 * sketch-based regression models
 */
object SketchedLinearRegression {

  import SketchedRegression._

  var dimension=2
  var inputDatasetResolution=2
  val numHeavyHitters = 300
  val epsilon = 0.0001
  val delta = 0.1
  val alpha = 0.5
  val numIterations=100
  val sketch: CMSketch = new CMSketch(delta, epsilon, numHeavyHitters);
  val inputspaceNormalizer = new Rounder(inputDatasetResolution);
  val stepsize =  inputspaceNormalizer.stepSize(dimension)
  val inputspace = new DynamicInputSpace(stepsize);

  def main(args: Array[String]): Unit = {
    val filename = "/home/kay/Dropbox/kay-rep/Uni-Berlin/Masterarbeit/datasets/linear_regression/dataset1"
    val is = new FileReader(new File(filename))

    sketch.alloc
    println("w="+sketch.w)
    println("d="+sketch.d)

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
      val discovery = new SketchDiscoveryEnumeration(sketch, inputspace, inputspaceNormalizer);
      //val discovery = new SketchDiscoveryHH(sketch);
      model = model - gradient_decent_step( new LinearRegressionModel(model), discovery )*alpha
      println(model)
    }
  }
}
