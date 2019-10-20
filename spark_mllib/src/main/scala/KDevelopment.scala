import java.io.{File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

import scala.math._

import scala.io.Source

object KDevelopment{

  val n = 2800

  def calculate(s1: String, s2: String): Double = {
    val a1 = s1.split(",")
    val a2 = s2.split(",")
    if (a1.length != a2.length){
      0.0
    }
    else {
      var res = 0.0
      for (i <- 0 until a1.length){
        res += pow(a1(i).toDouble - a2(i).toDouble, 2)
      }
      res = sqrt(res / pow(a1.length, 2))
      res
    }
  }

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val path : String = "src/main/sources/csdn_train_vectors.txt";
    val conf = new SparkConf().setAppName("CSDN_Plagiarism").setMaster("local[*]")
    val context = new SparkContext(conf)

    // Load and parse the data
    val data = context.textFile(path)
    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    // Cluster the data into several classes using KMeans
    val numClusters = n
    val numIterations = 20
    val clusters = KMeans.train(parsedData, numClusters, numIterations)

    val source = Source.fromFile("src/main/sources/csdn_development_vectors.txt", "UTF-8")
    val file = new File("src/main/sources/csdn_max_similarity.txt")
    file.createNewFile()
    val writer = new FileWriter(file, true)
    var maxOfAll = 0.0

    val lineIterator = source.getLines()
    while (lineIterator.hasNext) {
      val line = lineIterator.next()
      var maxSim = 0.0

      val index = clusters.predict(Vectors.dense(line.split(',').map(_.toDouble)))
      val subSource = Source.fromFile("src/main/sources/csdn_categories/csdn_category" + index + ".txt", "UTF-8")
      val subLineIterator = subSource.getLines()
      while (subLineIterator.hasNext){
        val subLine = subLineIterator.next()
        val sim = calculate(line, subLine)
        if (sim > maxSim){
          maxSim = sim
        }
      }
      writer.write(maxSim.toString + "\n")
      if (maxSim > maxOfAll){
        maxOfAll = maxSim
      }
    }
    writer.write("MAX VALUE: " + maxOfAll + "\n")
    writer.close()
  }
}
