package bigdata_sketching.main.scala.algorithms.sketches


import breeze.linalg._

class SketchedMatrix[V]( val rows: Int,
                              val cols: Int,
                              //val data: Array[V],
                              //val offset: Int,
                              //val majorStride: Int,
                              val isTranspose: Boolean = false,
                              val delta : Double,
                              val epsilon : Double,
                              val k : Int )
      extends Matrix[V] with MatrixLike[V, SketchedMatrix[V]] with Serializable {

  val sketch = new CMSketch( delta, epsilon, k  )
  sketch.alloc


  //def apply(i: Int, j: Int) : V
  def apply(row: Int, col: Int) = {
    if(row < - rows || row >= rows) throw new IndexOutOfBoundsException((row,col) + " not in [-"+rows+","+rows+") x [-"+cols+"," + cols+")")
    if(col < - cols || col >= cols) throw new IndexOutOfBoundsException((row,col) + " not in [-"+rows+","+rows+") x [-"+cols+"," + cols+")")

    sketch.get(""+row+""+col).asInstanceOf[V]
  }

  //implicit def toV( x : Int ) : V = x
  def update(row: Int, col: Int, e: V) : Unit = {
    sketch.update(""+row+""+col, e.asInstanceOf[Float] )
  }

  def copy: SketchedMatrix[V]  = {
    val sm = new SketchedMatrix[V](rows, cols, false, delta,epsilon,k)
    sm.sketch.set_hashfunctions(sketch.hashfunctions)
    sm
  }

  def repr: SketchedMatrix[V] = this

  def activeIterator: Iterator[((Int, Int), V)] = null

  def activeValuesIterator: Iterator[V] = null

  def activeKeysIterator: Iterator[(Int, Int)] = null

  def activeSize = sketch.size
}
