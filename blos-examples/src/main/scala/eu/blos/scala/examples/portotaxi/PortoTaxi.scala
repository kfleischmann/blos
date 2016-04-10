package eu.blos.scala.examples.portotaxi


import eu.blos.scala.inputspace.Vectors.DoubleVector
import eu.blos.scala.sketches._
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace._
import java.io.{PrintWriter, File, FileReader}
import eu.blos.scala.inputspace.Vectors.DoubleVector
import scala.collection.mutable
import eu.blos.scala.inputspace.Vectors.DoubleVector


object PortoTaxi {
  val datapath = "/home/kay/Dropbox/kay-rep/Uni-Berlin/Masterarbeit/Sources/blos/datasets/portotaxi/"

  val shortTripLength = 10
  var inputDatasetResolution=5
  val numHeavyHitters = 10
  val epsilon = 0.000001
  val delta = 0.01
  val sketch: CMSketch = new CMSketch(delta, epsilon, numHeavyHitters);
  val inputspaceNormalizer = new Rounder(inputDatasetResolution);
  val stepsize =  inputspaceNormalizer.stepSize(inputDatasetResolution)
  //val inputspace = new DynamicInputSpace(stepsize);
  //val inputspace = new StaticInputSpace( DoubleVector(41.15704, -8.63103), DoubleVector(41.15885, -8.62781), stepsize)
  //val inputspace = new StaticInputSpace( DoubleVector(41.1592, -8.6250), DoubleVector(41.1569, -8.6320), stepsize)
  //val inputspace = new StaticInputSpace( DoubleVector(41.15704, -8.63103), DoubleVector(41.15885, -8.62781), stepsize)


  val window = DoubleVector(0.01, 0.01 )
  //val center = DoubleVector(41.157864, -8.629112)
  //val center = DoubleVector(41.157864, -8.629112)
  val center = DoubleVector(41.143858, -8.612260)
  val inputspace = new StaticInputSpace( center-window, center+window, stepsize)


  def main(args: Array[String]): Unit = {
    val filename = datapath+"taxi2.tsv"
    val is = new FileReader(new File(filename))

    sketch.alloc
    println("w="+sketch.w)
    println("d="+sketch.d)

    skeching(sketch, new CSVIterator(is, "\t"), inputspaceNormalizer,  0, 24, false )
    is.close()

    write_sketch(datapath+"sketch/14/short/", sketch, inputspace, inputspaceNormalizer, stepsize )
    //learning
  }

  def skeching(sketch : CMSketch, dataset : CSVIterator, normalizer : InputSpaceNormalizer[DoubleVector], hourFrom : Int, hourTo : Int, longtrip : Boolean ) {
    val i = dataset.iterator
    while( i.hasNext ){
      val values = i.next
      if( values(dataset.getHeader.get("hour").get).toInt >= hourFrom && values(dataset.getHeader.get("hour").get).toInt <= hourTo ) {
        val lat = values(dataset.getHeader.get("lat").get)
        val lon = values(dataset.getHeader.get("lon").get)
        val duration = values(dataset.getHeader.get("duration").get).toInt
        val islong : Boolean = if(duration >= shortTripLength ) true else false

        // check if gps data is valid
        if(lat.length>0 && lon.length>0) {
          val vec = (normalizer.normalize(DoubleVector(lat.toDouble, lon.toDouble)))
          //if(islong == longtrip ) {
            sketch.update(vec.toString)
            //println(vec.toString)
            inputspace.update(vec)
          //}//if
        }//if
      }//if
    }//while
  }

  def learning {
    val hhdiscovery = new SketchDiscoveryHHIterator(sketch);
    while(hhdiscovery.hasNext){
      val item = hhdiscovery.next
      println( item.vector.toString+" => "+item.count )
    }

    val enumdiscovery = new SketchDiscoveryEnumerationIterator(sketch, inputspace, inputspaceNormalizer);
    while(enumdiscovery.hasNext){
      val item = enumdiscovery.next
      println( item.vector.toString+" => "+item.count )
    }
  }

  def write_sketch( output:String, sketch:CMSketch, inputspace : InputSpace[DoubleVector], inputspaceNormalizer : InputSpaceNormalizer[DoubleVector], stepsize : DoubleVector ) = {
    if( output.length >0) {
      new File(output).mkdirs()
      val outHHInputSpace = new PrintWriter( output + "/hh-input-space")
      val outEnumInputSpace = new PrintWriter( output + "/enumerated-input-space")
      val outHH = new PrintWriter( output + "/heavyhitters")
      val mapHH = new mutable.HashMap[String, Long]()

      val hhIt = new DiscoveryStrategyHH(sketch).iterator
      while (hhIt.hasNext) {
        val item = hhIt.next
        mapHH.put(item.vector.toString, item.count)
        outHH.write(item.vector.toString+"=>"+item.count)
        outHH.write("\n")
      }

      outHH.close()

      val enumIt = new DiscoveryStrategyEnumeration(sketch, inputspace, inputspaceNormalizer).iterator
      while (enumIt.hasNext) {
        val item = enumIt.next
        val pos = item.vector

        outEnumInputSpace.write(pos.elements.mkString(" ").concat(" ").concat(item.count.toString))
        outEnumInputSpace.write("\n")

        if (mapHH.contains(item.vector.toString)) {
          outHHInputSpace.write(pos.elements.mkString(" ").concat(" ").concat(item.count.toString))
          outHHInputSpace.write("\n")
        } else {
          outHHInputSpace.write(pos.elements.mkString(" ").concat(" ").concat("0"))
          outHHInputSpace.write("\n")
        }
      }
      outHHInputSpace.close()
      outEnumInputSpace.close()
    }
  }
}

