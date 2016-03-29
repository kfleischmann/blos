package eu.blos.scala.ml.regression

import eu.blos.scala.sketches._
import java.io.{ File,  FileReader}
import eu.blos.scala.inputspace.{Vectors, DataSetIterator, DynamicInputSpace}
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace.Vectors.DoubleVector

case class Config(
 input:String="",
 output:String="",
 epsilon:Double=0.0,
 delta:Double=0.0,
 alpha:Double=0.5,
 numIterations:Int=100,
 numHeavyHitters:Int=200,
 dimension:Int=2,
 inputDatasetResolution:Int=2,
 discovery:String="hh");
/**
 * sketch-based regression models
 */
object SketchedLinearRegression {

  import SketchedRegression._

  def main(args: Array[String]): Unit = {

    val parser = new scopt.OptionParser[Config]("regression") {
      head("Sketch-based Regression")

      opt[String]('i', "input") required() action {
        (x, c) => c.copy(input = x) }text("datset input")

      opt[String]('o', "output")  valueName("<file>") action {
        (x, c) => c.copy(output = x) }  text("output location")

      opt[String]('s', "sketch") required() valueName("<epsilon>:<delta>") action {
        (x, c) =>
          c.copy( delta = x.split(":")(1).toDouble).copy( epsilon = x.split(":")(0).toDouble)
      } text("sketch size")

    }

    // parser.parse returns Option[C]
    parser.parse(args, Config()) map { config =>
      run(config)
    } getOrElse {
      // arguments are bad, usage message will have been displayed
      System.exit(1)
    }
  }

  def run(config:Config) {

    val is = new FileReader(new File(config.input))
    println(config.epsilon)
    println(config.delta)

    val sketch = new CMSketch(config.delta, config.epsilon, config.numHeavyHitters);
    val inputspaceNormalizer = new Rounder(config.inputDatasetResolution);
    val stepsize =  inputspaceNormalizer.stepSize(config.dimension)
    val inputspace = new DynamicInputSpace(stepsize);

    // select discovery strategy and provide iterators
    var discovery : DiscoveryStrategy = null
    if(config.discovery == "hh") {
      discovery = new DiscoveryStrategyEnumeration(sketch, inputspace, inputspaceNormalizer);
    }
    if(config.discovery == "enumeration") {
      discovery = new DiscoveryStrategyHH(sketch);
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

      model = model - gradient_decent_step( new LinearRegressionModel(model), discoveryStrategy.iterator )*alpha
      println(model)
    }
  }
}
