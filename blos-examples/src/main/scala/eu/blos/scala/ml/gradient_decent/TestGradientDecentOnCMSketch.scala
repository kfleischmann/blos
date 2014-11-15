package eu.blos.scala.ml.gradient_decent


/**
 * This class loads a dataset into an count-min sketch and
 * applies and linear-regresion with gradient-decent using
 * the count min sketch
 *
 *
 * this should very the mechanics of an gradient-decent with
 * an underlying count-min sketch
 */

import scala.io.Source
import eu.blos.scala.algorithms.sketches.CMSketch


// sample_1 - sampleId
// sample_2 - X-value
// sample_3 - Y-value

class TestGradientDecentOnCMSketch( dataset : List[(Int,List[Double],Double)] ) {
  var model : Array[Double] = new Array[Double](2)

  def getY( i : Integer ) ={
    dataset(i)._3
  }

  def getX( i: Integer, j : Integer ) = {
    dataset(i)._2(j)
  }
  def getX( i: Integer ) = {
    dataset(i)._2
  }

  def gradient_decent( model : Array[Double], dataset :List[(Int,List[Double],Double)] ) = {
    val new_model = new Array[Double](2)

    val alpha = 0.05
    var errors_x1=0.0
    var errors_x2=0.0

    for( i <- Range(0,dataset.length)){
      errors_x1 += (model.zip(getX(i)).map(x=>x._1*x._2).sum - getY(i)) *getX(i,0)
      errors_x2 += (model.zip(getX(i)).map(x=>x._1*x._2).sum - getY(i)) *getX(i,1)
    }


    model(0)  = model(0) - alpha * (1.0/dataset.length) * errors_x1
    model(1)  = model(1) - alpha * (1.0/dataset.length) * errors_x2

    model
  }

  def run = {
    val sketch1 = new CMSketch(0.1, 0.0001)
    val sketch2 = new CMSketch(0.1, 0.0001)

    //sketch1.alloc
    //sketch2.alloc

    // y = m*x +b
    var model = new Array[Double](2)
    model(0) = 0  // b
    model(1) = 0  // m

    for(x <- Range(0,500)) {
      model = gradient_decent(model, dataset)
      println( model(0)+","+model(1) )
    }//for
  }
}

object TestGradientDecentOnCMSketch extends Serializable {
  def main(args:Array[String]){
    val filename = args(0)
    val dataset = Source.fromFile(filename).getLines().map( x => {
      val fields = x.split(",")
      //(SampleId, X-Values, Y
      (fields(0).toInt, List(1.0) ++ fields(1).split(" ").map(x=>x.toDouble), fields(2).toDouble )
    }).toList



    new TestGradientDecentOnCMSketch(dataset).run
  }
}