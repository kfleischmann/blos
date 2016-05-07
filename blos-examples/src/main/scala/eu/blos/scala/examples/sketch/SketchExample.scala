package eu.blos.scala.examples.sketch

import eu.blos.scala.inputspace.Vectors.DoubleVector
import eu.blos.scala.sketches._
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace.{InputSpace, InputSpaceNormalizer, DataSetIterator, DynamicInputSpace}
import java.io.{PrintWriter, File, FileReader}
import scala.collection.mutable
import eu.blos.scala.inputspace.Vectors.DoubleVector


/**
 * sketch-based regression models
 * depending on the regression model func a linear or a logistic regression is applied
 */
object SketchExample {

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
    val filename = "/path/do/dataset"
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
    val discoveryHH = new SketchDiscoveryHHIterator(sketch);

    while(discoveryHH.hasNext){
      val item = discoveryHH.next
      println( item.vector.toString+" => "+item.count )
    }

    val discoveryEnum = new SketchDiscoveryEnumerationIterator(sketch, inputspace, inputspaceNormalizer);
    while(discoveryEnum.hasNext){
      val item = discoveryEnum.next
      println( item.vector.toString+" => "+item.count )
    }

  }
}
