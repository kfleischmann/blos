package bigdata_sketching.main.scala.algorithms.sketches


class DistrubutedSketch(delta : Double, epsilon : Double, k : Int )  extends Serializable  {
  val sketch_mask = new CMSketch( delta, epsilon, k  )
  def new_partial_sketch : CMSketch = {
    val s = new CMSketch ( delta, epsilon, k  )
    s.set_hashfunctions( sketch_mask.get_hashfunctions )
    s
  }

  def combine( sketches:CMSketch* ) = {
    val s = new_partial_sketch
    s.alloc
    for( sketch <- sketches ){
      s.mergeWith(sketch)
    }
    s
  }
}
