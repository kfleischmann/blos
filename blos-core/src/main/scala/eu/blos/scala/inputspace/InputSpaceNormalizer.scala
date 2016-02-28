package eu.blos.scala.inputspace

trait InputSpaceNormalizer[T] {
  def normalize(vec:T): T;
  def getMax() : T;
  def getMin() : T;
  def update(vec:T);
  def iterator() : Iterator[T];
  def getRandom() : T;
  def getTotalElements() : Long;
}
