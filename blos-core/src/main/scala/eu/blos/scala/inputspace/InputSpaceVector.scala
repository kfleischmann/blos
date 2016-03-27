package eu.blos.scala.inputspace

trait InputSpaceVector[N] {
  type Self <: InputSpaceVector[N]
  def *(x: N): Self
  def +(x: N): Self
  def /(x: N): Self
  def -(x: N): Self
  def apply(i: Int): N
  def length: Int
  override def toString(): String = {
    val sb = new StringBuilder(getClass().getSimpleName())
    sb.append('(')
    var i = 0
    while (i < length) {
      if (i > 0) sb.append(',')
      sb.append(apply(i).toString())
      i += 1
    }
    sb.append(')')
    sb.toString()
  }
  def spaceKey = toString
}