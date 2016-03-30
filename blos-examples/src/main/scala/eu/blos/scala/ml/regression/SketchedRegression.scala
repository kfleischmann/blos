package eu.blos.scala.ml.regression

import eu.blos.scala.sketches._
import eu.blos.scala.inputspace.{InputSpace, Vectors, InputSpaceNormalizer, DataSetIterator}
import java.io.{PrintWriter, File}
import scala.collection.mutable
import eu.blos.scala.sketches.InputSpaceElement
import eu.blos.scala.inputspace.Vectors.DoubleVector

object SketchedRegression {

  case class Config(
         input:String="",
         output:String="",
         epsilon:Double=0.0,
         delta:Double=0.0,
         alpha:Double=0.5,
         numIterations:Int=100,
         numHeavyHitters:Int=200,
         dimension:Int=2,
         inputspaceResolution:Int=2,
         discovery:String="hh");


  trait TransformFunc {
    def apply(x:DoubleVector) : DoubleVector;
  }

  trait Model[T] {
    def predict(x:DoubleVector) : T;
  }

  abstract case class RegressionModel(model:DoubleVector) extends Model[Double] {
    def gradient(item:InputSpaceElement, d:Int) : Double
  }


  class LinearRegressionModel(model:DoubleVector) extends RegressionModel(model) {
    def predict(x:DoubleVector) : Double = {
      x*model
    }
    def gradient(item:InputSpaceElement, d:Int) : Double = {
      val y = item.vector.elements(0)
      val x = DoubleVector(1.0).append(item.vector.tail)
      (y - predict(x))*(-x.elements(d)) * item.count
    }
  }

  class LogisticRegressionModel(model:DoubleVector) extends RegressionModel(model) {
    def predict(x:DoubleVector) : Double = {
      1.0 / ( 1.0 + scala.math.exp(  -(x*model) ))
    }

    def gradient(item:InputSpaceElement, d:Int) : Double = {
      val y = item.vector.elements(0)
      val x = DoubleVector(1.0).append(item.vector.tail)
      (y - predict(x))*x.elements(d) * item.count
    }
  }

  def skeching(sketch : CMSketch, inputspace : InputSpace[DoubleVector], dataset : DataSetIterator, t: TransformFunc, normalizer : InputSpaceNormalizer[DoubleVector] ) {
    val i = dataset.iterator
    while( i.hasNext ){
      val vec = normalizer.normalize( t.apply(i.next))
      sketch.update(vec.toString )
      inputspace.update(vec)
    }
  }

  def gradient_decent_step(regression : RegressionModel, discovery:Iterator[InputSpaceElement] ) : DoubleVector = {
    var total_freq : Long = 0L
    var gradient = Vectors.EmptyDoubleVector(regression.model.length)*0.0
    while(discovery.hasNext){
      val item = discovery.next

      // for each dimension
      for( d <- Range(0,regression.model.length)) {
        gradient.elements(d) += (regression.gradient(item, d))
      }
      total_freq += item.count
    }
    gradient /= total_freq.toDouble

    gradient
  }

  def init(args: Array[String]): Config = {
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

      opt[String]('y', "discovery")   action {
        (x, c) =>
          c.copy( discovery = x )
      } text("discovery strategy. hh or enumeration")

      opt[Int]('d', "dimension") required()  action {
        (x, c) =>
          c.copy( dimension = x )
      } text("inputspace dimension")

      opt[Int]('n', "iterations") required()  action {
        (x, c) =>
          c.copy( numIterations = x )
      } text("number of iterations")

      opt[Int]('n', "resolution") required()  action {
        (x, c) =>
          c.copy( inputspaceResolution = x )
      } text("input space resolution")

      opt[Int]('H', "num-heavyhitters") action {
        (x, c) =>
          c.copy( numHeavyHitters = x )
      } text("number of heavy hitters")
    }

    // parser.parse returns Option[C]
    parser.parse(args, Config()) map { config =>
      config
    } getOrElse {
      // arguments are bad, usage message will have been displayed
      System.exit(1)
      null
    }
  }

  def write_sketch(config : Config, sketch:CMSketch, inputspace : InputSpace[DoubleVector], inputspaceNormalizer : InputSpaceNormalizer[DoubleVector], stepsize : DoubleVector ) = {
    if(config.output.length >0) {
      new File(config.output).mkdir()

      val outHH = new PrintWriter(config.output + "/hh")
      val outEnum = new PrintWriter(config.output + "/enumeration")

      val mapHH = new mutable.HashMap[String, Long]()

      val hhIt = new DiscoveryStrategyHH(sketch).iterator
      while (hhIt.hasNext) {
        val item = hhIt.next
        mapHH.put(item.vector.toString, item.count)
      }

      val enumIt = new DiscoveryStrategyEnumeration(sketch, inputspace, inputspaceNormalizer).iterator
      while (enumIt.hasNext) {
        val item = enumIt.next
       // val pos = DoubleVector(item.vector.elements.zip(stepsize.elements).map(x => scala.math.round(x._1 / x._2).toInt.toDouble))
        val pos = item.vector

        outEnum.write(pos.elements.mkString(" ").concat(" ").concat(item.count.toString))
        outEnum.write("\n")

        if (mapHH.contains(item.vector.toString)) {
          outHH.write(pos.elements.mkString(" ").concat(" ").concat(item.count.toString))
          outHH.write("\n")
        } else {
          outHH.write(pos.elements.mkString(" ").concat(" ").concat("0"))
          outHH.write("\n")
        }
      }
      outHH.close()
      outEnum.close()
    }
  }
}
