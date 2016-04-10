package eu.blos.scala.examples.portotaxi


import eu.blos.scala.inputspace.Vectors.DoubleVector
import eu.blos.scala.sketches.{SketchDiscoveryHHIterator, CMSketch}
import eu.blos.scala.inputspace.normalizer.Rounder
import eu.blos.scala.inputspace.{CSVIterator, InputSpaceNormalizer, DataSetIterator, DynamicInputSpace}
import java.io.{File, FileReader}


object PortoTaxi {

  trait TransformFunc {
    def apply(x:DoubleVector) : DoubleVector;
  }

  var inputDatasetResolution=4
  val numHeavyHitters = 10
  val epsilon = 0.0001
  val delta = 0.01
  val sketch: CMSketch = new CMSketch(epsilon, delta, numHeavyHitters);
  val inputspaceNormalizer = new Rounder(inputDatasetResolution);
  val stepsize =  inputspaceNormalizer.stepSize(inputDatasetResolution)
  val inputspace = new DynamicInputSpace(stepsize);


  def main(args: Array[String]): Unit = {
    val filename = "/home/kay/Dropbox/kay-rep/Uni-Berlin/Masterarbeit/Sources/blos/datasets/portotaxi/taxi2.tsv"
    val is = new FileReader(new File(filename))

    sketch.alloc

    skeching(sketch,
      new CSVIterator(is, "\t"),
      // skip first column (index)
      new TransformFunc() { def apply(x: DoubleVector) = x.tail},
      inputspaceNormalizer
    )
    is.close()

    learning
  }

  def skeching(sketch : CMSketch, dataset : CSVIterator, t: TransformFunc, normalizer : InputSpaceNormalizer[DoubleVector] ) {
    val i = dataset.iterator
    while( i.hasNext ){
      val values = i.next
      if( values(dataset.getHeader.get("hour").get).toInt == 14) {

       


        //val vec = normalizer.normalize( t.apply(i.next))
        //sketch.update(vec.toString )
        //inputspace.update(vec)
      }//if
    }
  }

  def learning {
    //val discovery = new SketchDiscoveryEnumeration(sketch, inputspace, inputspaceNormalizer);
    val discovery = new SketchDiscoveryHHIterator(sketch);

    while(discovery.hasNext){
      val item = discovery.next
      println( item.vector.toString+" => "+item.count )
    }
  }
}

