package scala.eu.blos.scala

import collection.mutable.Stack
import org.scalatest._
import eu.blos.scala.inputspace.Vectors.DoubleVector
import eu.blos.scala.inputspace.Vectors

class VectorSpec extends FlatSpec with GivenWhenThen {
  val vectors = List(
    DoubleVector( 5.0, 1.0, 2.0, 3.0, -4.0),
    DoubleVector( 3.0, 1.0,-2.0, 3.0, 0.0),
    DoubleVector(-3.0, 0.0, 0.0, 0.0, 0.0)
  );

  val resultMin = DoubleVector(-3.0, 0.0, -2.0, 0.0, -4.0 )
  val resultMax = DoubleVector( 5.0, 1.0,  0.0, 3.0,  0.0 )

  "MinSpaceVectors" should "find component wise minimal vector" in {
    Vectors.MinSpaceVector(vectors) == resultMin
  }


  "MinSpaceVectors" should "find component wise maxmimal vector" in {
    Vectors.MaxSpaceVector(vectors) == resultMax
  }
}
