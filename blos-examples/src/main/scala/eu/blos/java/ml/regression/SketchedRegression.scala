package eu.blos.java.ml.regression

import eu.blos.scala.sketches.CMSketch
import eu.blos.scala.inputspace.Vectors.DoubleVector
import java.io.{File, BufferedReader,FileReader}
import eu.blos.scala.inputspace.InputSpaceNormalizer


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

class DataSetIterator(filename:String) extends Iterable[DoubleVector] {
  var is = new FileReader(new File(filename))
  var br = new BufferedReader( is );

  def iterator = new Iterator[DoubleVector] {
    var line = br.readLine();
    def hasNext : Boolean = line != null
    def next : DoubleVector = {
      if(hasNext) {
        DoubleVector(line.split(" ").map(x => x.toDouble))
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
  var sketch : CMSketch;
  var model : RegressionModel = new LinearRegressionModel( DoubleVector(1.0, 0.0) );

  def main(args: Array[String]): Unit = {

    sketch = new CMSketch( epsilon, delta, numHeavyHitters );

    sketch.alloc();

    total_size = sketch.alloc_size();

    buildSketch(sketch, new DataSetIterator(file), new TransformFunc(){ def apply(x:DoubleVector)=x})

  def buildSketch(cms: CMSketch, dataset : DataSetIterator, t: TransformFunc, normalizer : InputSpaceNormalizer ) {
  }

  def regression_model(){

  }
}
