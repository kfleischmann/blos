package eu.blos.scala.algorithms.sketches

import eu.blos.java.api.common.{Sketch, DistributedSketch}

class DistributedCMSketch(delta : Double, epsilon : Double, k : Int )  extends Serializable with DistributedSketch  {

  val sketch_mask = new CMSketch( delta, epsilon, k  )

  def new_partial_sketch : CMSketch = {
    sketch_mask.clone_mask
  }
}