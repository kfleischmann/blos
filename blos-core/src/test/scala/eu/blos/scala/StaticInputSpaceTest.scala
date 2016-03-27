package eu.blos.scala

import org.scalatest._
import eu.blos.scala.inputspace.Vectors.DoubleVector
import eu.blos.scala.inputspace.{StaticInputSpace}

class StaticInputSpaceSpec extends FlatSpec with GivenWhenThen {
  val min =  DoubleVector( 0.0, 0.0 );
  val max =  DoubleVector( 1.0, 1.0 );
  val stepsize =  DoubleVector( 0.1, 0.1 );
  val inside = List (
    DoubleVector( 0.5, 0.5 ),
    DoubleVector( 1.0, 1.0 ),
    DoubleVector( 0.0, 0.0 )
  );
  val outside = List(
    DoubleVector( -0.5, 0.5 ),
    DoubleVector( 0.5, -0.5 )
  );
//  "inside vectors" should "be sinide " in {
//  }

//  "outside vectors" should "be sinide " in {
//  }
  "iteration" should " iterate over all possible values " in {
    val it = new StaticInputSpace(min,max,stepsize).iterator
    var count = 0
    while( it.hasNext){
      it.next
      count = count + 1
    }
    assert( count == 121 )
  }
}