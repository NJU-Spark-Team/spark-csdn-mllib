import java.io.{File, FileWriter}

import KDevelopment.{calculate, n}
import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.KMeans
import org.apache.spark.mllib.linalg.Vectors

import scala.collection.mutable
import scala.io.Source
import scala.math.{pow, sqrt}

object KTest{

  val similarity = 45.204026092816115;

  def count(sim: Double, map: mutable.HashMap[Double, Int]): Unit ={
    map.foreach(e => {
      if (sim > e._1){
        map.put(e._1, e._2 + 1)
      }
    })
  }

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

    val source = Source.fromFile("src/main/sources/csdn_test_vectors.txt", "UTF-8")
    val file = new File("src/main/sources/csdn_max_similarity_test.txt")
    file.createNewFile()
    val writer = new FileWriter(file, true)

    val map = new mutable.HashMap[Double, Int]
    for (i <- 1 until 10){
      val ratio = i.toDouble / 10.0
      map.put(ratio, 0)
    }

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
      count(maxSim, map)
      writer.write(maxSim.toString + "\n")
    }
    map.foreach(e => {
      writer.write("Number of recordswhich similarity is larger than " + e._1.toString + " is " + e._2.toString()
        + ", with the percentage of " + (e._2.toDouble * 100.0 / 5192.0) + ".\n")
    })
    writer.close()
  }
}
