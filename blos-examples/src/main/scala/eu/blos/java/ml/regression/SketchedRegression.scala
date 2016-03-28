package eu.blos.java.ml.regression

import eu.blos.scala.sketches.CMSketch
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
  val numHeavyHitters = 500
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
      new TransformFunc() {
        def apply(x: DoubleVector) = x
      },
      inputspaceNormalizer
    )
    is.close()
    learning
    println(stepsize.toString)
  }

  def skeching(cms: CMSketch, dataset : DataSetIterator, t: TransformFunc, normalizer : InputSpaceNormalizer[DoubleVector] ) {
    val i = dataset.iterator
    while( i.hasNext ){
      val vec = normalizer.normalize(i.next.tail)
      cms.update(vec.toString )
      inputspace.update(vec)
    }
  }

  def learning {
    val it = inputspace.iterator
    while(it.hasNext){
      val v = inputspaceNormalizer.normalize(it.next)
      println(v + "freq:"+sketch.get(v.toString))
    }
  }

  def regression_model(){
  }
}
