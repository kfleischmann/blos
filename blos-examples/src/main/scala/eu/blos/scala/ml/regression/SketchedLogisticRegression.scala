package eu.blos.scala.ml.regression

import eu.blos.scala.sketches._
import java.io.{ File,  FileReader}
import eu.blos.scala.inputspace.{Vectors, DataSetIterator, DynamicInputSpace, InputSpaceNormalizer}
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace.Vectors.DoubleVector


/**
 * sketch-based logistic regression models
 */
object SketchedLogisticRegression {

  import SketchedRegression._

  def main(args: Array[String]): Unit = run( init(args) )

  def run(config:Config) {
    println("Sketch-based Logistic Regression")

    val is = new FileReader(new File(config.input))
    val sketch = new CMSketch(config.delta, config.epsilon, config.numHeavyHitters);
    val inputspaceNormalizer = new Rounder(config.inputspaceResolution);
    val stepsize =  inputspaceNormalizer.stepSize(config.dimension)
    val inputspace = new DynamicInputSpace(stepsize);

    // select discovery strategy and provide iterators
    var discovery : DiscoveryStrategy = null
    if(config.discovery == "hh") {
      println("discovery=hh")
      discovery = new DiscoveryStrategyHH(sketch);
    }
    if(config.discovery == "enumeration") {
      println("discovery=enumeration")
      discovery = new DiscoveryStrategyEnumeration(sketch, inputspace, inputspaceNormalizer);
    }

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
    learning(sketch, config.numIterations, config.alpha, discovery )
  }

  def learning(sketch:CMSketch, iterations:Int, alpha:Double, discoveryStrategy:DiscoveryStrategy) {
    var model = Vectors.EmptyDoubleVector(2)+1
    for(x <- Range(0,iterations) ){
      model = model - gradient_decent_step( new LogisticRegressionModel(model), discoveryStrategy.iterator )*alpha
      println(model)
    }
  }
}
