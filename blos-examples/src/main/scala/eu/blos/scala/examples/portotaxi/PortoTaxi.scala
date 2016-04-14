package eu.blos.scala.examples.portotaxi


import eu.blos.scala.sketches._
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace._
import java.io.{PrintWriter, File, FileReader}
import scala.collection.mutable
import eu.blos.scala.inputspace.Vectors.DoubleVector

case class Config(
  input:String="",
  output:String="",
  epsilon:Double=0.0,
  delta:Double=0.0,
  numHeavyHitters:Int=200,
  inputspaceResolution:Int=3,
  discovery:String="hh",
  writeSketch : Boolean = false,
  verbose : Boolean = false,
  center : DoubleVector = DoubleVector(),
  window : DoubleVector = DoubleVector(),
  shortTripLength : Int = 0, // in minutes
  radius : Double = 0.0 // in meters
);

object PortoTaxi {
  /**
   * Helper method
   * compute distance between two gps coordinates
   * (lat1,lon1) - (lat2, lon2)
   * @param pointA
   * @param pointB
   * @return
   */
  def haversineDistance(pointA: (Double, Double), pointB: (Double, Double)): Double = {
    val R = 6371000 // in m
    val deltaLat = math.toRadians(pointB._1 - pointA._1)
    val deltaLong = math.toRadians(pointB._2 - pointA._2)
    val a = math.pow(math.sin(deltaLat / 2), 2) + math.cos(math.toRadians(pointA._1)) * math.cos(math.toRadians(pointB._1)) * math.pow(math.sin(deltaLong / 2), 2)
    val greatCircleDistance = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    R * greatCircleDistance
  }

  val TRIPTYPE_LONG         = 1
  val TRIPTYPE_SHORT        = 2
  val TRIPTYPE_LONGSHORT    = 3
  var total_error_count     = 0L
  var total_counts          = 0L
  var config : Config = null

  def main(args: Array[String]): Unit = {
    config = init(args)
    run(config)
  }

  def run(config:Config) {
    // prepare data structures
    val filename = config.input
    val sketch: CMSketch = new CMSketch(config.delta, config.epsilon, config.numHeavyHitters);
    val inputspaceNormalizer = new Rounder(config.inputspaceResolution);
    val stepsize =  inputspaceNormalizer.stepSize(config.inputspaceResolution)
    val dyn_inputspace = new DynamicInputSpace(stepsize);
    val stat_inputspace = new StaticInputSpace( inputspaceNormalizer.normalize(config.center-config.window), inputspaceNormalizer.normalize(config.center+config.window), stepsize)

    sketch.alloc

    println("w="+sketch.w)
    println("d="+sketch.d)

    skeching(sketch, new CSVIterator(new FileReader(new File(filename)), "\t"), inputspaceNormalizer, dyn_inputspace );

    for( h <- Range(0,24) ) {

      val countLong = count_parzen_window_sketch(sketch, stat_inputspace, config.center, config.radius, h, TRIPTYPE_LONG, inputspaceNormalizer )
      val countShort = count_parzen_window_sketch(sketch, stat_inputspace, config.center, config.radius, h, TRIPTYPE_SHORT, inputspaceNormalizer )
      val total = countShort + countLong

      val sketch_probLong = (countLong.toDouble / total.toDouble)
      val sketch_probShort =(countShort.toDouble / total.toDouble)

      println("sketch-result="+List(h,countLong,countShort,sketch_probLong,sketch_probShort).mkString(","))

      val real_counts = count_parzen_window_real (new CSVIterator(new FileReader(new File(filename)), "\t"), config.center, config.radius, inputspaceNormalizer, h, h )
      // result (countLong,countShort)

      val real_probLong = (real_counts._2.toDouble / (real_counts._1+real_counts._2).toDouble)
      val real_probShort = (real_counts._2.toDouble / (real_counts._1+real_counts._2).toDouble)

      println("real-result="+List(h,real_counts._1,real_counts._2,real_probLong,real_probShort).mkString(","))

      val errors_long = Math.abs(countLong - real_counts._1)
      val errors_short = Math.abs(countShort - real_counts._2)

      total_error_count = total_error_count + errors_long + errors_short
      total_counts = total_counts + total
    }
    println("total counts: "+total_counts)
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
            if (duration <= config.shortTripLength) {
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

  def count_parzen_window_sketch(sketch : CMSketch, inputspace : InputSpace[DoubleVector], center : DoubleVector, radius : Double, hour : Int, tripType : Int, normalizer : InputSpaceNormalizer[DoubleVector]  ) = {
    var freq_count = 0L
    val enumWindow = inputspace.iterator
    while (enumWindow.hasNext) {
      val item = enumWindow.next
      val vector = normalizer.normalize( DoubleVector( item.elements(0), item.elements(1), tripType.toDouble, hour.toDouble) )
      val count = sketch.get(vector.toString)
      val coordinates = item
      val distance = haversineDistance( (center.elements(0), center.elements(1)), (coordinates.elements(0), coordinates.elements(1))).toInt
      if( distance < radius ){
        freq_count += count
      }
    }
    freq_count
  }

  def skeching(sketch : CMSketch, dataset : CSVIterator, normalizer : InputSpaceNormalizer[DoubleVector], inputspace : InputSpace[DoubleVector] ) = {
    val i = dataset.iterator
    while( i.hasNext ){
      val values = i.next
      val hour = values(dataset.getHeader.get("hour").get).toInt
      val lat = values(dataset.getHeader.get("lat").get)
      val lon = values(dataset.getHeader.get("lon").get)
      val duration = values(dataset.getHeader.get("duration").get).toInt

      val tripType =
        duration match {
          case x if x > config.shortTripLength => TRIPTYPE_LONG
          case x if x <= config.shortTripLength => TRIPTYPE_SHORT
        }

      // check if gps data is valid
      if(lat.length>0 && lon.length>0 ) {
        val vec = (normalizer.normalize(DoubleVector(lat.toDouble, lon.toDouble, tripType.toDouble, hour.toDouble)))

        // update sketch
        sketch.update(vec.toString)

         // update inputspace
        inputspace.update(vec)
      }//if
    }//while
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

  def init(args: Array[String]): Config = {
    val parser = new scopt.OptionParser[Config]("Taxi Trip Length Predicton") {
      head("Sketch-based Taxi Trip Length Prediction")

      opt[String]('i', "input") required() action {
        (x, c) => c.copy(input = x) }text("datset input")

      opt[String]('o', "output")  valueName("<file>") action {
        (x, c) => c.copy(output = x) }  text("output location")

      opt[String]('s', "sketch") required() valueName("<epsilon>:<delta>") action {
        (x, c) =>
          c.copy( delta = x.split(":")(0).toDouble).copy( epsilon = x.split(":")(1).toDouble)
      } text("sketch size, delta:epsilon")

      opt[String]('c', "center")   action {
        (x, c) =>
          c.copy( center = DoubleVector( x.split(":").map(_.toDouble) ))
      } text("center lat:lon")

      opt[String]('w', "window")   action {
        (x, c) =>
          c.copy( window = DoubleVector( x.split(":").map(_.toDouble) ))
      } text("window lat:lon")
      opt[Boolean]('v', "verbose")  action {
        (x, c) =>
          c.copy( verbose = x )
      } text("enable verbose mode")

      opt[Boolean]('W', "write-sketch")  action {
        (x, c) =>
          c.copy( writeSketch = x )
      } text("write sketch into output path")
      opt[Int]('R', "resolution") required()  action {
        (x, c) =>
          c.copy( inputspaceResolution = x )
      } text("input space resolution")

      opt[Int]('T', "shorttriplength") required()  action {
        (x, c) =>
          c.copy( shortTripLength = x )
      } text("max length of a short trip in minutes")

      opt[Int]('H', "num-heavyhitters") action {
        (x, c) =>
          c.copy( numHeavyHitters = x )
      } text("number of heavy hitters")

      opt[Int]('r', "radius") action {
        (x, c) =>
          c.copy( radius = x )
      } text("radius of the parzen window in meters")

    }

    // parser.parse returns Option[C]
    parser.parse(args, Config()) map { config =>
      config
    } getOrElse {
      // arguments are bad, usage message will have been displayed
      System.exit(1)
      null
    }
  }

}

