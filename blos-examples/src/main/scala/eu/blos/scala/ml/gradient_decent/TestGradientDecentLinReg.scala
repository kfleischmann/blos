package eu.blos.scala.ml.gradient_decent


/**
 * This class loads a dataset into an count-min sketch and
 * applies and linear-regression with gradient-decent using
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

class TestGradientDecentLinReg( val alpha : Double, val dataset : Array[(Int,List[Double],Double)] ) {
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

  def getXY( i : Integer, k : Integer ) = {
    getY(i) * getX(i, k)
  }

  def getXX( i : Integer, j : Integer, k : Integer ) = {
    getX(i,j)*getX(i, k)
  }


  def gradient_decent( model : Array[Double], dataset :Array[(Int,List[Double],Double)] ) = {
    val new_model = new Array[Double](2)

    var errors_x1=0.0
    var errors_x2=0.0

    for( i <- Range(0,dataset.length)){

      errors_x1 += - getXY(i, 0) //getY(i) * getX(i, 0)
      errors_x2 += - getXY(i, 1) //getY(i) * getX(i, 1)

      for( j <- Range(0,model.length)) {
        errors_x1 += model(j)*getXX(i, j, 0)  //getXX(i,j)*getX(i, 0)
        errors_x2 += model(j)*getXX(i, j, 1)  //getXX(i,j)*getX(i, 1)
      }

    }
    model(0)  = model(0) - alpha * (1.0/dataset.length) * errors_x1
    model(1)  = model(1) - alpha * (1.0/dataset.length) * errors_x2

    model
  }

  def run = {
    // y = m*x +b
    var model = new Array[Double](2)
    model(0) = 0  // b
    model(1) = 0  // m

    for(x <- Range(0,30000)) {
      model = gradient_decent(model, dataset)
      println( model(0)+","+model(1) )
    }//for
  }
}


class TestGradientDecentLinRegWithSketch( override val alpha : Double, override val dataset : Array[(Int,List[Double],Double)] )
  extends TestGradientDecentLinReg(alpha, dataset) {

  val sketch_labels = new CMSketch(0.1 /*factor*/, 0.0001 /*prob*/);
  val sketch_samples = new CMSketch(0.1 /*factor*/, 0.00001 /*prob*/);

  sketch_labels.alloc
  sketch_samples.alloc

  println(sketch_labels.size)

  sketch

  def sketch = {
    println("sketching")

    for( i <- Range(0,dataset.length)) {
      sketch_labels.update( i+"-"+0, (getY(i)*getX(i,0)).toFloat )
      sketch_labels.update( i+"-"+1, (getY(i)*getX(i,1)).toFloat )

      for( j <- Range(0,model.length)) {
        sketch_samples.update( i+"-"+j+"-"+0, (getX(i,0)*getX(i,1)).toFloat )
        sketch_samples.update( i+"-"+j+"-"+1, (getX(i,1)*getX(i,0)).toFloat )
      }
    }
  }

  override def getXY( i : Integer, k : Integer ) = {
    // getY(i) * getX(i, k)
    sketch_labels.get(i+"-"+k)
  }

  override def getXX( i : Integer, j : Integer, k : Integer ) = {
    //getX(i,j)*getX(i, k)
    sketch_samples.get(i+"-"+j+"-"+k)
  }


}

object TestGradientDecentLinReg extends Serializable {
  def main(args:Array[String]){
    val filename = args(0)
    val dataset = Source.fromFile(filename).getLines().map( x => {
      val fields = x.split(",")
      //(SampleId, X-Values, Y
      (fields(0).toInt, List(1.0) ++ fields(1).split(" ").map(x=>x.toDouble), fields(2).toDouble )
    }).toArray
    println("dataset loaded")


    new TestGradientDecentLinRegWithSketch(0.000005, dataset).run
  }
}