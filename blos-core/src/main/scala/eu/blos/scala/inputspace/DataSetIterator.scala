package eu.blos.scala.inputspace

import java.io.{BufferedReader, InputStreamReader}
import eu.blos.scala.inputspace.Vectors.DoubleVector

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