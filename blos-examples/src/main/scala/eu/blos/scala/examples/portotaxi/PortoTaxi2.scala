package eu.blos.scala.examples.portotaxi

import eu.blos.scala.sketches._
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace._
import java.io.{PrintWriter, File, FileReader}
import scala.collection.mutable
import eu.blos.scala.inputspace.Vectors.DoubleVector


object PortoTaxi2 {

  def haversineDistance(pointA: (Double, Double), pointB: (Double, Double)): Double = {
    val R = 6371000 // in m
    val deltaLat = math.toRadians(pointB._1 - pointA._1)
    val deltaLong = math.toRadians(pointB._2 - pointA._2)
    val a = math.pow(math.sin(deltaLat / 2), 2) + math.cos(math.toRadians(pointA._1)) * math.cos(math.toRadians(pointB._1)) * math.pow(math.sin(deltaLong / 2), 2)
    val greatCircleDistance = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    R * greatCircleDistance
  }

  val datapath = "/home/kay/Dropbox/kay-rep/Uni-Berlin/Masterarbeit/Sources/blos/datasets/portotaxi/"
  val TRIPTYPE_LONG         = 1
  val TRIPTYPE_SHORT        = 2
  val TRIPTYPE_LONGSHORT    = 3
  val shortTripLength = 10
  var inputDatasetResolution=3
  val numHeavyHitters = 10
  val epsilon = 0.0003
  val delta = 0.1

  var numDistinctElementsLongTrips :Long = 0L
  var numDistinctElementsShortTrips :Long = 0L

  val bloomLongTrips = new BloomFilter(1000000, 2, "SHA-1")
  val bloomShortTrips = new BloomFilter(1000000, 2, "SHA-1")

  val sketchLongTrips: CMSketch = new CMSketch(delta, epsilon, numHeavyHitters);
  val sketchShortTrips: CMSketch = new CMSketch(delta, epsilon, numHeavyHitters);

  val inputspaceNormalizer = new Rounder(inputDatasetResolution);
  val stepsize =  inputspaceNormalizer.stepSize(inputDatasetResolution)

  //val inputspace = new DynamicInputSpace(stepsize);
  //val inputspace = new StaticInputSpace( DoubleVector(41.15704, -8.63103), DoubleVector(41.15885, -8.62781), stepsize)
  //val inputspace = new StaticInputSpace( DoubleVector(41.1592, -8.6250), DoubleVector(41.1569, -8.6320), stepsize)
  //val inputspace = new StaticInputSpace( DoubleVector(41.15704, -8.63103), DoubleVector(41.15885, -8.62781), stepsize)

  val window = DoubleVector(0.003, 0.003 )
  //val center = DoubleVector(41.157864, -8.629112)
  //val center = DoubleVector(41.157864, -8.629112)
  //val center = DoubleVector(41.143858, -8.612260)
  //val center = DoubleVector( 41.157395, -8.627763 )
  // hbf
  //val center = DoubleVector(41.1492123,-8.5877372);
  // airport
  val center = DoubleVector(41.237021, -8.669633);



  // watery
  //val center = DoubleVector(41.142471,-8.7047457)

  //
  //val center = DoubleVector(41.158985, -8.630705)

  val inputspace = new StaticInputSpace( inputspaceNormalizer.normalize(center-window), inputspaceNormalizer.normalize(center+window), stepsize)

  val filename = datapath+"taxi2.tsv"
  val radius = 500 // in meter

  var total_error_count = 0L
  var total_counts = 0L

  sketchShortTrips.alloc
  sketchLongTrips.alloc

  println("w="+sketchShortTrips.w)
  println("d="+sketchShortTrips.d)

  def main(args: Array[String]): Unit = {

    for( h <- Range(0,24) ) {
      sketchShortTrips.reset
      sketchLongTrips.reset

      numDistinctElementsLongTrips = skeching_hours(sketchLongTrips, new CSVIterator(new FileReader(new File(filename)), "\t"), inputspaceNormalizer, h, h+1, TRIPTYPE_LONG,bloomLongTrips, numDistinctElementsLongTrips)
      numDistinctElementsShortTrips = skeching_hours(sketchShortTrips, new CSVIterator(new FileReader(new File(filename)), "\t"), inputspaceNormalizer, h, h+1, TRIPTYPE_SHORT, bloomShortTrips, numDistinctElementsShortTrips)

      write_sketch(datapath + "sketch/"+h+"/long/", sketchLongTrips, inputspace, inputspaceNormalizer, stepsize)
      write_sketch(datapath + "sketch/"+h+"/short/", sketchShortTrips, inputspace, inputspaceNormalizer, stepsize)

      val countLong = count_parzen_window(sketchLongTrips, inputspace, center, radius)
      val countShort = count_parzen_window(sketchShortTrips, inputspace, center, radius)
      val total = countShort + countLong

      println("("+countLong+","+countShort+")")

      println("sketch: "+h+" prob. short:" + (countShort.toDouble / total.toDouble))
      println("sketch: "+h+" prob. long:" + (countLong.toDouble / total.toDouble))

      val real_counts = count_parzen_window_real (new CSVIterator(new FileReader(new File(filename)), "\t"), center, radius, inputspaceNormalizer, h, h+1 )
      println(real_counts)

      println("real: "+h+" prob. short:" + (real_counts._2.toDouble / (real_counts._1+real_counts._2).toDouble))
      println("real: "+h+" prob. long:" + (real_counts._1.toDouble / (real_counts._1+real_counts._2).toDouble))

      val errors_long = Math.abs(countLong - real_counts._1)
      val errors_short = Math.abs(countShort - real_counts._2)
      total_error_count = total_error_count + errors_long + errors_short
      total_counts = total_counts + total

      println("errors short:" + errors_short)
      println("errors long:" + errors_long)
      println("errors total: "+total_error_count)
      println("total counts: "+total_counts)
      println("-----------------------------")
    }
  }

  def count_parzen_window_real(dataset: CSVIterator, center : DoubleVector, radius : Double, normalizer : InputSpaceNormalizer[DoubleVector], hourFrom : Int, hourTo : Int) = {
    var freq_short = 0L
    var freq_long = 0L
    val i = dataset.iterator
    while( i.hasNext ) {
      val values = i.next
      if( values(dataset.getHeader.get("hour").get).toInt >= hourFrom && values(dataset.getHeader.get("hour").get).toInt <= hourTo ) {
        val lat = values(dataset.getHeader.get("lat").get)
        val lon = values(dataset.getHeader.get("lon").get)
        val duration = values(dataset.getHeader.get("duration").get).toInt

        if (lat.length > 0 && lon.length > 0) {
          val coordinates = (normalizer.normalize(DoubleVector(lat.toDouble, lon.toDouble)))
          val distance = haversineDistance((center.elements(0), center.elements(1)), (coordinates.elements(0), coordinates.elements(1))).toInt
          if (distance < radius) {
            //println(coordinates.toString+", distance: "+distance)

            if (duration <= shortTripLength) {
              freq_short += 1
            } else {
              freq_long += 1
            }
          }
        }
      }
    }
    (freq_long, freq_short)
  }

  def count_parzen_window(sketch : CMSketch, inputspace : InputSpace[DoubleVector], center : DoubleVector, radius : Double) = {
    var freq_count = 0L
    val enumWindow = new DiscoveryStrategyEnumeration(sketch, inputspace, inputspaceNormalizer).iterator
    while (enumWindow.hasNext) {
      val item = enumWindow.next
      //println(item)
      val coordinates = item.vector

      val distance = haversineDistance( (center.elements(0), center.elements(1)), (coordinates.elements(0), coordinates.elements(1))).toInt
      //println(distance)
      if( distance < radius ){
        freq_count += item.count
      }
    }
    freq_count
  }

  def skeching_hours(sketch : CMSketch, dataset : CSVIterator, normalizer : InputSpaceNormalizer[DoubleVector], hourFrom : Int, hourTo : Int, filter_tripType : Int, bloom : BloomFilter, distinctCounterIn : Long) = {
   var distinctCounter = distinctCounterIn
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

            if(!bloom.contains(vec.toString)){
              distinctCounter = distinctCounter + 1
            } else {
              bloom.add(vec.toString)
            }
          }//if
        }//if
      }//if
    }//while
    distinctCounter
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

