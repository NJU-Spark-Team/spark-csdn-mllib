import java.io.{File, FileWriter}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.mllib.clustering.{KMeans, KMeansModel}
import org.apache.spark.mllib.linalg.Vectors

object KComparison{


  def main(args: Array[String]): Unit = {
    val writer = new FileWriter("src/main/sources/csdn_kmeans_model_comparison.txt", true)

    Logger.getLogger("org.apache.spark").setLevel(Level.ERROR)
    val path : String = "src/main/sources/csdn_train_vectors.txt";
    val conf = new SparkConf().setAppName("CSDN_Plagiarism").setMaster("local[*]")
    val context = new SparkContext(conf)

    // Load and parse the data
    val data = context.textFile(path)
    val parsedData = data.map(s => Vectors.dense(s.split(',').map(_.toDouble))).cache()

    var n = 1
    while (n <= 10000){
      // Cluster the data into several classes using KMeans
      val numClusters = n
      val numIterations = 20
      val clusters = KMeans.train(parsedData, numClusters, numIterations)

      // Evaluate clustering by computing Within Set Sum of Squared Errors
      writer.write("K is: " + n + "\n")
      val wssse = clusters.computeCost(parsedData)
      writer.write("Within Set Sum of Squared Errors = "+ wssse + "\n")
      writer.write("\n")
      writer.flush()
      n += 100
    }

    context.stop()


  }
}
