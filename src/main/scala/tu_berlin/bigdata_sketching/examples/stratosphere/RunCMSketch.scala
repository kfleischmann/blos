package main.scala.tu_berlin.bigdata_sketching.examples.stratosphere


import eu.stratosphere.api.scala._
import eu.stratosphere.api.scala.operators._
import eu.stratosphere.client.LocalExecutor

object RunCMSketch {
  def main(args: Array[String]) {
    val input = TextFile(textInput)

    val words = input.flatMap { _.split(" ") map { (_, 1) } }

    val counts = words.groupBy { case (word, _) => word }
      .reduce { (w1, w2) => (w1._1, w1._2 + w2._2) }

    val output = counts.write(wordsOutput, CsvOutputFormat())
    val plan = new ScalaPlan(Seq(output))

    LocalExecutor.execute(plan)
  }
}
