package eu.blos.scala.ml.regression

import eu.blos.scala.sketches._
import java.io.{PrintWriter, File, FileReader}
import eu.blos.scala.inputspace._
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace.Vectors.DoubleVector

/**
 * sketch-based linear regression models
 */
object SketchedLinearRegression {

  var config : Config = null
  import SketchedRegression._

  def main(args: Array[String]): Unit = {
    config = init(args)
    run(config)
  }

  def run(config:Config) {
    println("Sketch-based Linear Regression")
    val is = new FileReader(new File(config.input))
    val sketch = new CMSketch(config.delta, config.epsilon, config.numHeavyHitters);
    val inputspaceNormalizer = new Rounder(config.inputspaceResolution);
    val stepsize =  inputspaceNormalizer.stepSize(config.dimension)
    var inputspace : InputSpace[DoubleVector] =  new DynamicInputSpace(stepsize)

    if (config.min.length > 0 && config.max.length > 0) {
      println("use static input space min:" + new DoubleVector(config.min.split(",").map(x => x.toDouble)) + ", max:"+ new DoubleVector(config.max.split(",").map(x => x.toDouble)))
      inputspace = new StaticInputSpace(new DoubleVector(config.min.split(",").map(x => x.toDouble)), new DoubleVector(config.max.split(",").map(x => x.toDouble)), stepsize)
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


    if(!config.skiplearning) {
      learning(sketch, config.numIterations, config.alpha, new DiscoveryStrategyEnumeration(sketch, inputspace, inputspaceNormalizer), config.output + "/model-results-enumeration")
      learning(sketch, config.numIterations, config.alpha, new DiscoveryStrategyHH(sketch), config.output + "/model-results-hh")
    }

    if(config.writeSketch)
      write_sketch(config, sketch, inputspace, inputspaceNormalizer, stepsize )
  }

  def learning(sketch:CMSketch, iterations:Int, alpha:Double, discoveryStrategy:DiscoveryStrategy, modelOutput : String ) {

    val output = new PrintWriter(modelOutput)
    var model = Vectors.EmptyDoubleVector(2)+1
    for(x <- Range(0,iterations) ){
      model = model - gradient_decent_step( new LinearRegressionModel(model), discoveryStrategy.iterator )*alpha
      output.write(model.elements.mkString(" "))
      output.write("\n")
      println(model)
    }
    println( modelOutput+ ":"+model)

    output.close()
  }
}
