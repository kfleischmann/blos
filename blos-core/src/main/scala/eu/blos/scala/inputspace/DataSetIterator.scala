package eu.blos.scala.inputspace

import java.io.{BufferedReader, InputStreamReader}
import eu.blos.scala.inputspace.Vectors.DoubleVector
import scala.io.Source

class DataSetIterator(is:InputStreamReader, delimiter:String = " ") extends Iterable[DoubleVector] {
  def iterator = new Iterator[DoubleVector] {
    var br = new BufferedReader( is );
    var line = br.readLine();
    def hasNext : Boolean = line != null
    def next : DoubleVector = {
      if(hasNext) {
        val v = DoubleVector(line.split(delimiter).map(x => x.toDouble))
        line = br.readLine();
        v
      } else {
        null
      }
    }
  }
}

class CSVIterator(is:InputStreamReader, delimiter:String = " ", hasHeader:Boolean = true) extends Iterable[Array[String] ] {
  val br = new BufferedReader(is)
  var header: scala.collection.mutable.Map[String,Int] = readHeader
  def getHeader = header
  def readHeader:scala.collection.mutable.Map[String,Int] = {
    val h = scala.collection.mutable.Map.empty[String,Int]
    if (hasHeader) {
      val fields = br.readLine().split(delimiter)
      for( f <- fields.zipWithIndex ) h.put(f._1, f._2 )
      h
    } else {
      null
    }
  }
  def iterator = new Iterator[Array[String] ] {
    var line = br.readLine();
    def hasNext: Boolean = line != null
    def next: Array[String]  = {
      if (hasNext) {
        val l = line
        line = br.readLine()
        l.split(delimiter)
      } else {
        null
      }
    }
  }
}

class CSVBufferedIterator(filename:String, delimiter:String = " ", hasHeader:Boolean = true) extends Iterable[Array[String] ] {
  var fileLines = Source.fromFile(filename).getLines.toArray
  var header: scala.collection.mutable.Map[String,Int] = readHeader
  def getHeader = header
  def readHeader:scala.collection.mutable.Map[String,Int] = {
    val h = scala.collection.mutable.Map.empty[String,Int]
    if (hasHeader) {
      val fields = fileLines.head.split(delimiter)
      for( f <- fields.zipWithIndex ) h.put(f._1, f._2 )
      fileLines = fileLines.tail
      h
    } else {
      null
    }
  }
  def iterator = {
    new Iterator[Array[String]] {
      var max = fileLines.length
      var pos = 0
      def hasNext: Boolean = pos < max
      def next: Array[String] = {
        if (hasNext) {
          val l = fileLines(pos)
          pos = pos + 1
          l.split(delimiter)
        } else {
          null
        }
      }
    }
  }
}