package ssi.d7a

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.junit.Test

class SparkTest {
  @Test
  def test {
   val logFile = "/opt/spark-1.1.0-bin-hadoop2.4/README.md"
//    val conf = new SparkConf().setAppName("Simple Application")
   println(System.getProperty("java.class.path"))
   val sc = new SparkContext("local", "test")
   val logData = sc.textFile(logFile, 2).cache()
   val numAs = logData.filter(line => line.contains("a")).count()
   val numBs = logData.filter(line => line.contains("b")).count()
   println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}