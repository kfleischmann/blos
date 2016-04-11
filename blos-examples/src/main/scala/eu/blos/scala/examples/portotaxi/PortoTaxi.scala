package eu.blos.scala.examples.portotaxi


import eu.blos.scala.sketches._
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace._
import java.io.{PrintWriter, File, FileReader}
import scala.collection.mutable
import eu.blos.scala.inputspace.Vectors.DoubleVector


object PortoTaxi {
  val datapath = "/home/kay/Dropbox/kay-rep/Uni-Berlin/Masterarbeit/Sources/blos/datasets/portotaxi/"

  val TRIPTYPE_LONG         = 1
  val TRIPTYPE_SHORT        = 2
  val TRIPTYPE_LONGSHORT    = 3


  val shortTripLength = 10
  var inputDatasetResolution=5
  val numHeavyHitters = 10
  val epsilon = 0.000007
  val delta = 0.1

  val sketchLongTrips: CMSketch = new CMSketch(delta, epsilon, numHeavyHitters);
  val sketchShortTrips: CMSketch = new CMSketch(delta, epsilon, numHeavyHitters);

  val inputspaceNormalizer = new Rounder(inputDatasetResolution);
  val stepsize =  inputspaceNormalizer.stepSize(inputDatasetResolution)
  //val inputspace = new DynamicInputSpace(stepsize);
  //val inputspace = new StaticInputSpace( DoubleVector(41.15704, -8.63103), DoubleVector(41.15885, -8.62781), stepsize)
  //val inputspace = new StaticInputSpace( DoubleVector(41.1592, -8.6250), DoubleVector(41.1569, -8.6320), stepsize)
  //val inputspace = new StaticInputSpace( DoubleVector(41.15704, -8.63103), DoubleVector(41.15885, -8.62781), stepsize)

  val window = DoubleVector(0.0005, 0.0005 )
  //val center = DoubleVector(41.157864, -8.629112)
  //val center = DoubleVector(41.157864, -8.629112)
  //val center = DoubleVector(41.143858, -8.612260)
  //val center = DoubleVector( 41.157395, -8.627763 )

  // hbf
  //val center = DoubleVector(41.1492123,-8.5877372);

  // water
  val center = DoubleVector(41.142471,-8.7047457)

  val inputspace = new StaticInputSpace( center-window, center+window, stepsize)


  def main(args: Array[String]): Unit = {
    val filename = datapath+"taxi2.tsv"
    val is = new FileReader(new File(filename))
    val radius = 0.001

    sketchShortTrips.alloc
    sketchLongTrips.alloc

    println("w="+sketchShortTrips.w)
    println("d="+sketchShortTrips.d)

    for( h <- Range(0,24) ) {
      sketchShortTrips.reset
      sketchLongTrips.reset

      skeching(sketchLongTrips, new CSVIterator(new FileReader(new File(filename)), "\t"), inputspaceNormalizer, h, h+1, TRIPTYPE_LONG)
      skeching(sketchShortTrips, new CSVIterator(new FileReader(new File(filename)), "\t"), inputspaceNormalizer, h, h+1, TRIPTYPE_SHORT)

      write_sketch(datapath + "sketch/"+h+"/long/", sketchLongTrips, inputspace, inputspaceNormalizer, stepsize)
      write_sketch(datapath + "sketch/"+h+"/short/", sketchShortTrips, inputspace, inputspaceNormalizer, stepsize)

      val countLong = count_parzen_window(sketchLongTrips, inputspace, center, radius)
      println(countLong)
      val countShort = count_parzen_window(sketchShortTrips, inputspace, center, radius)
      println(countShort)

      val total = countShort + countLong

      println(h+" prob. short:" + (countShort.toDouble / total.toDouble))
      println(h+" prob. long:" + (countLong.toDouble / total.toDouble))
    }
  }

  def skeching(sketch : CMSketch, dataset : CSVIterator, normalizer : InputSpaceNormalizer[DoubleVector], hourFrom : Int, hourTo : Int, filter_tripType : Int ) {
    val i = dataset.iterator
    while( i.hasNext ){
      val values = i.next
      if( values(dataset.getHeader.get("hour").get).toInt >= hourFrom && values(dataset.getHeader.get("hour").get).toInt <= hourTo ) {
        val lat = values(dataset.getHeader.get("lat").get)
        val lon = values(dataset.getHeader.get("lon").get)
        val duration = values(dataset.getHeader.get("duration").get).toInt
        val tripType =
          duration match {
            case x if x > shortTripLength => TRIPTYPE_LONG
            case x if x <= shortTripLength => TRIPTYPE_SHORT
          }
        // check if gps data is valid
        if(lat.length>0 && lon.length>0) {
          val vec = (normalizer.normalize(DoubleVector(lat.toDouble, lon.toDouble)))
          if( (tripType & filter_tripType) > 0 ) {
            sketch.update(vec.toString)
            inputspace.update(vec)
          }//if
        }//if
      }//if
    }//while
  }

  def count_parzen_window(sketch : CMSketch, inputspace : InputSpace[DoubleVector], center : DoubleVector, radius : Double) = {
    var freq_count = 0L
    val enumWindow = new DiscoveryStrategyEnumeration(sketch, inputspace, inputspaceNormalizer).iterator
    while (enumWindow.hasNext) {
      val item = enumWindow.next
      val coordinates = item.vector

      if( (center-coordinates)*(center-coordinates) <= radius ) {
        freq_count += item.count
      }
    }
    freq_count
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

