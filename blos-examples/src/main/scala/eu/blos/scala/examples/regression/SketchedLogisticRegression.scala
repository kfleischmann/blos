package eu.blos.scala.examples.regression

import eu.blos.scala.sketches._
import java.io.{PrintWriter, File, FileReader}
import eu.blos.scala.inputspace._
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace.Vectors.DoubleVector


/**
 * sketch-based logistic regression models
 */
object SketchedLogisticRegression {
  var config : Config = null
  import SketchedRegression._

  def main(args: Array[String]): Unit = {
    config = init(args)
    run(config)
  }


  def logistic_gradient_decent_step(regression : RegressionModel, discovery:Iterator[InputSpaceElement] ) : DoubleVector = {
    var total_freq : Long = 0L
    var gradient = Vectors.EmptyDoubleVector(regression.model.length)*0.0
    while(discovery.hasNext){
      val item = discovery.next
      // for each dimension
      for( d <- Range(0,regression.model.length)) {
        gradient.elements(d) -= (regression.gradient(item, d))
      }
      total_freq += item.count
    }
    gradient /= total_freq.toDouble

    gradient
  }

  def run(config:Config) {
    println("Sketch-based Logistic Regression")

    val is = new FileReader(new File(config.input))
    val sketch = new CMSketch(config.delta, config.epsilon, config.numHeavyHitters);
    val inputspaceNormalizer = new Rounder(config.inputspaceResolution);
    val stepsize =  inputspaceNormalizer.stepSize(config.dimension)
    var inputspace : InputSpace[DoubleVector] =  new DynamicInputSpace(stepsize)
    /*
    // select discovery strategy and provide iterators
    var discovery : DiscoveryStrategy = null
    if(config.discovery == "hh") {
      println("discovery=hh")
      discovery = new DiscoveryStrategyHH(sketch);
    }
    if(config.discovery == "enumeration") {
      println("discovery=enumeration")
      discovery = new DiscoveryStrategyEnumeration(sketch, inputspace, inputspaceNormalizer);
    }*/

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
      learning(sketch, config.numIterations, config.alpha, new DiscoveryStrategyHH(sketch), config.output + "/model-results-hh")
      learning(sketch, config.numIterations, config.alpha, new DiscoveryStrategyEnumeration(sketch, inputspace, inputspaceNormalizer), config.output + "/model-results-enumeration")
    }
    write_sketch(config.output, sketch, inputspace, inputspaceNormalizer, stepsize )
  }

  def learning(sketch:CMSketch, iterations:Int, alpha:Double, discoveryStrategy:DiscoveryStrategy, modelOutput : String ) {
    val output = new PrintWriter(modelOutput)
    var model = Vectors.EmptyDoubleVector(2)+1
    for(x <- Range(0,iterations) ){
      model = model - logistic_gradient_decent_step( new LogisticRegressionModel(model), discoveryStrategy.iterator )*alpha
      output.write(model.elements.mkString(" "))
      output.write("\n")
      println(model)
    }
    println( modelOutput+ ":"+model)
    output.close()
  }
}
