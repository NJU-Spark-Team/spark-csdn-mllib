import java.io.{File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

import scala.io.Source

object KDivision{

  val n = 2800;

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

    val writers = new Array[FileWriter](n)
    for (i <- 0 until n) {
      val file = new File("src/main/sources/csdn_categories/csdn_category" + i + ".txt")
      file.createNewFile()
      writers(i) = new FileWriter(file, true)
    }

    val source = Source.fromFile(path, "UTF-8")
    val lineIterator = source.getLines()
    while (lineIterator.hasNext) {
      val line = lineIterator.next()
      val index = clusters.predict(Vectors.dense(line.split(',').map(_.toDouble)))
      writers(index).write(line + "\n")
      writers(index).flush()
    }
    for (i <- 0 until n) {
      writers(i).close()
    }
  }
}
