package eu.blos.scala.examples.portotaxi


import eu.blos.scala.sketches._
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace._
import java.io.{PrintWriter, File, FileReader}
import scala.collection.mutable
import scala.collection.mutable.HashMap
import eu.blos.scala.inputspace.Vectors.DoubleVector
import scala.collection.mutable.ArrayBuffer

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
  radius : Double = 0.0, // in meters
  timeFrom  : Int = 0,
  timeTo    : Int = 24,
  timeStep  : Int = 1,
  eval_min  : DoubleVector = DoubleVector(),
  eval_max  : DoubleVector = DoubleVector()
);

object PortoTaxi {

  case class TaxiTripCount(realCount:Long,sketchCount:Long, false_positive : Long){
    def error = sketchCount - realCount
  }
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
  var total_real_counts     = 0L
  var total_sketch_counts   = 0L
  var total_false_positive_counts   = 0L
  var total_false_positive_items    = 0L
  var N = 0
  var space_items = 0
  var config : Config = null
  var sum_true_positive = 0
  var sum_false_positive = 0

  var sum_true_negative = 0
  var sum_false_negative = 0

  var total_queries = 0
  var sum_longTrips = 0

  def main(args: Array[String]): Unit = {
    config = init(args)
    run(config)
  }

  var sketch: CMSketch = null

  def run(config:Config) {
    // prepare data structures
    val filename = config.input
    val inputspaceNormalizer = new Rounder(config.inputspaceResolution);
    val stepsize = inputspaceNormalizer.stepSize(config.inputspaceResolution)
    val dyn_inputspace = new DynamicInputSpace(stepsize);


    sketch = new CMSketch(config.delta, config.epsilon, config.numHeavyHitters);

    sketch.alloc

    println("w="+sketch.w)
    println("d="+sketch.d)

    skeching(sketch, new CSVIterator(new FileReader(new File(filename)), "\t"), inputspaceNormalizer, dyn_inputspace );

    println("sketching finished")
    println("min="+dyn_inputspace.getMin.toString )
    println("max="+dyn_inputspace.getMax.toString )

    val realDataset = getRealDataset(config, inputspaceNormalizer)

    val max = config.eval_max
    val min = config.eval_min


    val stat_inputspace = new StaticInputSpace( min, max, stepsize)
    val it = stat_inputspace.iterator
    var space_items = 0
    while( it.hasNext){
      val c = inputspaceNormalizer.normalize(it.next)
      if(config.verbose) {
        println("evaluate center " + c)
      }
      evaluate (config, realDataset, sketch, inputspaceNormalizer, dyn_inputspace, stepsize, c, config.window )
      space_items = space_items + 1

      if(config.verbose) {
        if (space_items % 10 == 0) {
          println("evaluated input-space items: " + space_items)
          print_stats
        }
      }
    }

    print_stats
  }


  def print_stats = {
    println("evaluation enum input-space count:"+space_items)

    println("total errors: "+ (total_sketch_counts - total_real_counts)  )
    println("total real counts: "+total_real_counts)
    println("total sketch counts: "+total_sketch_counts)
    println("total false_positive counts: "+total_false_positive_counts)
    println("total false_positive items: "+total_false_positive_items)

    val error_per_query = (2*N/sketch.w)
    val total_estimated_error = error_per_query*space_items*24*2

    println("computed error per query: "+ error_per_query )
    println("computed total error estimate: "+ total_estimated_error )
    println("real error per query: "+ (total_sketch_counts - total_real_counts).toDouble / total_queries )

    println("sum false positive long-trips:"+sum_false_positive)
    println("sum queries:"+total_queries)
    println("sum long trips:"+sum_longTrips)
    println("precision:"+ (sum_true_positive.toDouble/(sum_true_positive+sum_false_positive).toDouble))
    println("recall:"+ (sum_true_positive.toDouble/(sum_false_negative+sum_true_positive).toDouble))
  }

  def getRealDataset(config:Config,inputspaceNormalizer : InputSpaceNormalizer[DoubleVector]) : HashMap[String, Int] =  {
    val dataset = new CSVIterator(new FileReader( new File(config.input)), "\t")
    val realdataset =  new HashMap[String, Int]
    var counter=0
    var total_size=0
    val dit = dataset.iterator
    while(dit.hasNext) {
      val values = dit.next
      val hour = values(dataset.getHeader.get("hour").get).toInt
      val lat = values(dataset.getHeader.get("lat").get)
      val lon = values(dataset.getHeader.get("lon").get)
      val duration = values(dataset.getHeader.get("duration").get).toInt
      val tripType =
        duration match {
          case x if x > config.shortTripLength => TRIPTYPE_LONG
          case x if x <= config.shortTripLength => TRIPTYPE_SHORT
        }
      if (lat.length > 0 && lon.length > 0) {
        val features = inputspaceNormalizer.normalize(DoubleVector(lat.toDouble, lon.toDouble, tripType, hour.toDouble))

        if (realdataset.contains(features.toString)) {
          realdataset.put(features.toString, realdataset.get(features.toString).get + 1)
        } //if
        else {
          N += 1
          realdataset.put(features.toString, 1)

          // size of counter
          total_size = total_size + 4

          // size of key
          total_size = total_size + (features.toString.length )
        }
      }
    }
    var counts = ArrayBuffer[Int]()
    var count_1 = 0
    var count_n1 = 0
    realdataset.foreach( keyVal => {
        counts+=keyVal._2
        if( keyVal._2 > 1 ) {
          count_n1 += 1
        } else {
          count_1 += 1
        }
      }
    )

    println("count 1:"+count_1)
    println("count >1:"+count_n1)
    println("hashmap was built with distinct keys: "+N +", "+((total_size.toDouble)/1024.0/1024.0)+" mb" )
    realdataset
  }

  def evaluate(config:Config, dataset: HashMap[String, Int], sketch: CMSketch, inputspaceNormalizer : InputSpaceNormalizer[DoubleVector], dyn_inputspace : DynamicInputSpace, stepsize : DoubleVector, center : DoubleVector, window : DoubleVector ) = {

    val stat_inputspace = new StaticInputSpace( inputspaceNormalizer.normalize(center-window), inputspaceNormalizer.normalize(center+window), stepsize)

    val result_writer = new PrintWriter( config.output+"_"+center.elements.mkString("_")+".tsv" )

    result_writer.write(List("hour","real_count_longtrips","real_count_short_trips","sketch_count_longstrips", "sketch_count_shorttrips").mkString("\t") )
    result_writer.write("\n")

    for( h <- Range( config.timeFrom, config.timeTo, config.timeStep) ) {

      val countLong = count_parzen_window(dataset, sketch, stat_inputspace, center, config.radius, h, h + config.timeStep, TRIPTYPE_LONG, inputspaceNormalizer)
      val countShort = count_parzen_window(dataset, sketch, stat_inputspace, center, config.radius, h, h + config.timeStep, TRIPTYPE_SHORT, inputspaceNormalizer)

      val sketch_count = countShort.sketchCount + countLong.sketchCount
      val sketch_probLong = (countLong.sketchCount.toDouble / sketch_count.toDouble)
      val sketch_probShort = (countShort.sketchCount.toDouble / sketch_count.toDouble)

      val real_count = countLong.realCount + countShort.realCount
      val real_probLong = (countLong.realCount.toDouble / real_count.toDouble)
      val real_probShort = (countShort.realCount.toDouble / real_count.toDouble)

      if (real_probLong > real_probShort ) {
        sum_longTrips += 1
      }

      // positive items: long trips
      // if long predicted, but reality is short
      if (sketch_probLong > sketch_probShort && real_probLong <= real_probShort ) {
        sum_false_positive += 1
      }

      // if long predicted, and reality is long
      if (sketch_probLong > sketch_probShort && real_probLong > real_probShort ) {
        sum_true_positive += 1
      }

      // negative items: short trips

      // if short predicted, but reality is long
      if (sketch_probLong <= sketch_probShort && real_probLong > real_probShort ) {
        sum_false_negative += 1
      }

      // if short predicted, and reality is short
      if (sketch_probLong <= sketch_probShort && real_probLong <= real_probShort ) {
        sum_true_negative += 1
      }

      total_queries += 1

      //println(real_probLong.toDouble +","+ sketch_probLong.toDouble)

      if(config.verbose) {
        println("real-result=" + List(h, countLong.realCount, countShort.realCount, sketch_probLong, sketch_probShort).mkString(","))
        println("sketch-result=" + List(h, countLong.sketchCount, countShort.sketchCount, real_probLong, real_probShort).mkString(","))
      }

      val errors_long = Math.abs(countLong.error)
      val errors_short = Math.abs(countShort.error)

      result_writer.write(List(h,countLong.realCount, countShort.realCount, countLong.sketchCount,countShort.sketchCount).mkString("\t") )
      result_writer.write("\n")

      total_real_counts   = total_real_counts + real_count
      total_sketch_counts = total_sketch_counts + sketch_count
      total_false_positive_counts = total_false_positive_counts + countLong.false_positive + countShort.false_positive

      if( countLong.false_positive>0)
        total_false_positive_items = total_false_positive_items +1
      if( countShort.false_positive>0)
        total_false_positive_items = total_false_positive_items +1
    }

    result_writer.close()
  }

  def count_parzen_window(dataset: HashMap[String, Int], sketch : CMSketch, inputspace : InputSpace[DoubleVector], center : DoubleVector, radius : Double, hourFrom : Int, hourTo : Int, tripType : Int, normalizer : InputSpaceNormalizer[DoubleVector]  ) = {
    var real_freq_count = 0L
    var sketch_freq_count = 0L
    var false_positive_count = 0L
    val enumWindow = inputspace.iterator
    while (enumWindow.hasNext) {
      val item = enumWindow.next
      val coordinates = item

      // value from sketch
      // valid distance to center
      val distance = haversineDistance( (center.elements(0), center.elements(1)), (coordinates.elements(0), coordinates.elements(1))).toInt
      if( distance < radius ){
        for( h <- Range(hourFrom,hourTo)){
          val vector = normalizer.normalize( DoubleVector( item.elements(0), item.elements(1), tripType.toDouble, h.toDouble) )

          // value from sketched data
          //if( dataset.contains(vector.toString) ) {
            val count = sketch.get(vector.toString)
            sketch_freq_count += count
          //}

          // value from real dataset
          if( dataset.contains(vector.toString) ){
            real_freq_count += dataset.get(vector.toString).get.toLong
          }//if
          else {
            // but found some in sketch
            if(count > 0) {
              false_positive_count += count
            }
          }
        }//for
      }//if
    }//while

    new TaxiTripCount(real_freq_count, sketch_freq_count, false_positive_count)
  }

  def skeching(sketch : CMSketch, dataset : CSVIterator, normalizer : InputSpaceNormalizer[DoubleVector], inputspace : InputSpace[DoubleVector] ) = {
    val keys =  new HashMap[String, Int]
    var distinct_keys = 0
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

        if(!keys.contains(vec.toString)){
          keys.put(vec.toString, 1 )
          distinct_keys = distinct_keys + 1
        }

        // update inputspace
        inputspace.update(vec)

      }//if
    }//while
    println("distinct keys in sketch: "+distinct_keys)
    distinct_keys
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

      opt[String]('E', "evaluate") required() action {
        (x, c) =>
          c.copy( eval_min = DoubleVector( x.split(",")(0).split(":").map(_.toDouble) )).copy( eval_max = DoubleVector( x.split(",")(1).split(":").map(_.toDouble) ))

      } text("evaluate window on map: lat_min:lon_min,lat_max:lon_max")

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

      opt[Double]('r', "radius") required() action {
        (x, c) =>
          c.copy( radius = x )
      } text("radius of the parzen window in meters")

      opt[String]('h', "hours") action {
        (x, c) =>
          c.copy( timeFrom = x.split(":")(0).toInt ).copy(timeTo = x.split(":")(1).toInt).copy(timeStep = x.split(":")(2).toInt)
      } text("from:to:step")
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