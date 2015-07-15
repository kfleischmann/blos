package eu.blos.scala.algorithms.sketches


import eu.blos.java.algorithms.sketches.field_normalizer.ZeroOneNormalizer

trait NormalizedSketch extends CMSketch {

  val normalizer = new ZeroOneNormalizer(22)

  abstract override def update( key : String ) {
    super.update( normalizer.normalize(key.toDouble).toString , 1L )
  }

  abstract override def get( key : String ) = {
    super.get( normalizer.normalize( key.toDouble).toString )
  }
}
