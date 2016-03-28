package eu.blos.scala.examples

import eu.blos.scala.inputspace.Vectors.DoubleVector
import eu.blos.scala.sketches.{SketchDiscoveryHH, CMSketch}
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace.{InputSpaceNormalizer, DataSetIterator, DynamicInputSpace}
import java.io.{File, FileReader}


/**
 * sketch-based regression models
 * depending on the regression model func a linear or a logistic regression is applied
 */
object SketchedRegression {

  trait TransformFunc {
    def apply(x:DoubleVector) : DoubleVector;
  }

  var inputDatasetResolution=2
  val numHeavyHitters = 10
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
    //val discovery = new SketchDiscoveryEnumeration(sketch, inputspace, inputspaceNormalizer);
    val discovery = new SketchDiscoveryHH(sketch);

    while(discovery.hasNext){
      val item = discovery.next
      println( item.vector.toString+" => "+item.count )
    }
  }
}
