package main.scala.tu_berlin.bigdata_sketching.examples.local

import main.scala.tu_berlin.bigdata_sketching.algoritms.sketches.{CMSketch, SketchedMatrix}
import breeze.linalg.DenseMatrix
import scala.util.Random

object TestSketchedMatrix {

  def main(args: Array[String]) {
    val m1 = new SketchedMatrix[Float](100,100,false,0.001, 0.005, 5)

    /*
    val rows = 10000
    val cols = 10000
    val dm = DenseMatrix.zeros[Float](rows,cols)

    val s = new CMSketch(Math.pow(10,-8), 0.00005, -1 )
    s.alloc
    println(s.size.toFloat/1024.0/1024.0)
    dm.activeKeysIterator.foreach( x=> {
      dm.update(x._1, x._2, new Random().nextFloat());
      s.update(x._1+"_"+x._2, dm.apply(x._1,x._2).toFloat);
      /*if(x._1<10 && x._2 < 10 ) {
        print(" " + x._1 + "_" + x._2 + "=" + dm.apply(x._1, x._2));
      }*/
      //println(" " + x._1 + "_" + x._2 + "=" + dm.apply(x._1, x._2));
      //if(x._1 % 10 == 0 ) println(" " + x._1 + "_" + x._2 + "=" + dm.apply(x._1, x._2));
    })
    println("")

    /* println("")
    dm.activeKeysIterator.foreach( x=> {
      if(x._1<10 && x._2 < 10 ) {
        print(" " + x._1 + "_" + x._2 + "=" + s.get(x._1 + "_" + x._2))
      }
    })
    println("")
    println("--------------------")
    println(s.size.toFloat/1024.0/1024.0)
    //s.print
    */

    */
  }
}
