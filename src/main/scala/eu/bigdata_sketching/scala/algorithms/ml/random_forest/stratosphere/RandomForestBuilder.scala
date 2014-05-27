package bigdata_sketching.main.scala.algorithms.ml.random_forest.stratosphere



import eu.stratosphere.client.LocalExecutor
import scala.util.Random
import java.io._
import org.apache.log4j.Level
import scala.io.Source
import eu.stratosphere.client.PlanExecutor
import eu.stratosphere.client.RemoteExecutor
import eu.stratosphere.core.fs.FileSystem
import eu.stratosphere.core.fs.Path
import java.net.URI
import eu.stratosphere.api.common.Plan
import eu.bigdata_sketching.scala.algorithms.ml.random_forest.stratosphere.DecisionTreeEvaluator

class RandomForestBuilder(val remoteJar : String = null,
                          val remoteJobManager : String = null,
                          val remoteJobManagerPort : Int = 0 ) {
  /**
   * Evaluates test data set based on the random forest model.
   *
   * @param inputPath Data to classify/evaluate. In the same format as training data.
   *
   * @param treePath The random forest model, created by
   * [[bigdataproject.scala.eu.stratosphere.ml.randomforest.RandomForestBuilder]].build()
   *
   * @param outputPath Classified data; format:
   * "[data item index], [classified label], [actual label from data item]"
   */
  def eval(inputFile: String, treeFile: String, outputFile: String) = {
    val fs : FileSystem = FileSystem.get(new URI(inputFile))
    val inputPath = inputFile
    val treePath = treeFile
    val outputPath = outputFile

    // prepare executor
    var ex : PlanExecutor = null
    if( remoteJar == null ){
      val localExecutor = new LocalExecutor();
      localExecutor.start()
      ex = localExecutor
      //LocalExecutor.setLoggingLevel(Level.ERROR)
      println("Stratosphere using local executor")
    } else {
      ex = new RemoteExecutor(remoteJobManager, remoteJobManagerPort, remoteJar );
      println("Stratosphere using remotee xecutor ip:"+remoteJobManager+" port:"+remoteJobManagerPort+" jar:"+remoteJar)
    }

    val plan = new DecisionTreeEvaluator().getPlan(inputPath, treePath, outputPath)
    val runtime = ex.executePlan(plan)

    var percentage = 0.0
    val src =Source.fromInputStream(fs.open(new Path(new URI(outputFile))))
    try {
      val lines = src.getLines.map(_.split(",").map(_.toInt)).toList

      System.out.println("statistics");
      System.out.println("total results: " + lines.length)
      val correct = lines.filter(x => x(1) == x(2)).length
      System.out.println("correct: " + correct)
      val wrong = lines.filter(x => x(1) != x(2)).length
      System.out.println("wrong: " + wrong)
      System.out.println("percentage: " + (correct.toDouble * 100 / lines.length.toDouble))
      percentage = (correct.toDouble * 100 / lines.length.toDouble)
    } finally {
      src.close()
    }
    percentage
  }
}