package eu.bigdata_sketching.scala.examples.local

import scala.util.Random
import breeze.linalg._
import breeze.plot._
import eu.bigdata_sketching.scala.algorithms.CMSketch

object TestSketchedMatrix {

  def main(args: Array[String]) {
    //val m1 = new SketchedMatrix[Float](100,100,false,0.001, 0.005, 5)

    val rows = 2
    val cols = 1000
    val dm = DenseMatrix.zeros[Float](rows,cols)
    val dm_rec = DenseMatrix.zeros[Float](rows,cols)
    val samples = getRandomSamples(cols, rows)
    val s = new CMSketch(Math.pow(10,-8), 0.00005, -1 )
    s.alloc
    println(s.size.toFloat/1024.0/1024.0)


    dm.activeKeysIterator.foreach( x=> {
      // generate random numbers
      dm.update(x._1, x._2, samples(x._1)(x._2) );
      s.update(x._1+"_"+x._2, dm.apply(x._1,x._2).toFloat);

    })

    dm.activeKeysIterator.foreach( x=> {
      dm_rec.update(x._1,x._2, s.get(x._1+"_"+x._2))
    })

    //val w = breeze.linalg.inv()*2*dm_rec.t*y

    val f = Figure()
    val p = f.subplot(0)
    val x = linspace(0.0,1.0)
    print(x)


    p += plot(x, x :^ 2.0)
    p += plot(x, x :^ 3.0, '.')
    p.xlabel = "x axis"
    p.ylabel = "y axis"
   }


  def getRandomSamples(samples:Int, features : Int ) = {
    Seq.fill(features,samples)(Random.nextFloat)
  }

  def getLinRegSamples(samples:Int) = {
    val x = linspace(0.0,1.0, samples)
    val y = x :* 5.0
    val dm = DenseMatrix.zeros[Float](samples,2)
    for(i<-0 until samples) {
      dm(0, i) = x(i).toFloat
      dm(1, i) = y(i).toFloat
    }
    dm
  }
}
