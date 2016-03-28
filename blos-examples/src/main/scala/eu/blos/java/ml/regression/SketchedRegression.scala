package eu.blos.java.ml.regression

import eu.blos.scala.sketches.{SketchDiscoveryHH, SketchDiscoveryEnumeration, CMSketch}
import eu.blos.scala.inputspace.Vectors.DoubleVector
import java.io.{InputStreamReader, File, BufferedReader, FileReader}
import eu.blos.scala.inputspace.{DynamicInputSpace, InputSpaceNormalizer}
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

class DataSetIterator(is:InputStreamReader, delimiter:String = " ") extends Iterable[DoubleVector] {
  def iterator = new Iterator[DoubleVector] {
    var br = new BufferedReader( is );
    var line = br.readLine();
    def hasNext : Boolean = line != null
    def next : DoubleVector = {
      if(hasNext) {
        val v = DoubleVector(line.split(delimiter).map(x => x.toDouble))
        line = br.readLine();
        v
      } else {
        null
      }
    }
  }
}


/**
 * sketch-based regresison models
 * depending on the regression model func a linear or a logistic regression is applied
 */
object SketchedRegression {
  val epsilon = 0.0001
  val delta = 0.01
  val numHeavyHitters = 10
  var model : RegressionModel = new LinearRegressionModel( DoubleVector(1.0, 0.0) );
  val inputspaceNormalizer = new Rounder(2);
  val stepsize =  inputspaceNormalizer.stepSize(2)
  val inputspace = new DynamicInputSpace(stepsize);
  val sketch: CMSketch = new CMSketch(epsilon, delta, numHeavyHitters);


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

  def regression_model(){
  }
}
