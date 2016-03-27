package eu.blos.scala.inputspace

trait InputSpaceNormalizer[T] {
  def normalize(vec:T): T;
  def stepSize(dim:Int): T;
}
