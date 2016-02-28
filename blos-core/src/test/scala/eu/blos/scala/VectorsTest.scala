package scala.eu.blos.scala

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
  val resultMax = DoubleVector( 5.0, 1.0,  2.0, 3.0,  0.0 )

  "MinSpaceVectors" should "find component wise minimal vector" in {
    assert( Vectors.MinSpaceVector(vectors) == resultMin )
  }


  "MinSpaceVectors" should "find component wise maxmimal vector" in {
    assert( Vectors.MaxSpaceVector(vectors) == resultMax )
  }

  "Smaller voectors" should " be smaller than bigger" in {
    assert( DoubleVector(-1.0, 0.0, 2.0 ) < DoubleVector(0.0, 0.0, 0.0 ) == false )
    assert( DoubleVector(-1.0, 0.0, 2.0 ) < DoubleVector(0.0, 0.0, 2.0 ) == true )
  }
}
