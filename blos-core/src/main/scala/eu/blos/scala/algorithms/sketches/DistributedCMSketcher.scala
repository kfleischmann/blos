package eu.blos.scala.algorithms.sketches

import eu.blos.java.api.common.{Sketch, DistributedSketcher}

class DistributedCMSketcher(delta : Double, epsilon : Double, k : Int )  extends Serializable with DistributedSketcher  {

  val sketch_mask = new CMSketch( delta, epsilon, k  )

  def new_partial_sketch : Sketch = {
    val s = new CMSketch ( delta, epsilon, k  )
    s.set_hashfunctions( sketch_mask.get_hashfunctions )
    s
  }

  /*def combine( sketches:CMSketch* ) = {
    val s = new_partial_sketch
    s.alloc
    for( sketch <- sketches ){
      s.mergeWith(sketch)
    }
    s
  }*/
}