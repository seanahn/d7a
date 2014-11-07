package ssi.d7a

import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.joda.time.DateTime
import org.junit.Test

class SparkTest {
    @Test
    def test {
        val path = s"${System.getProperty("user.dir")}/testResources/assets/asset_service_apj_complete_340434_INC_India_Distributor_201408211304_PART000.txt.20140821130439"
    
        val sc = new SparkContext("local", "test")
        val logData = sc.textFile(path, 2).cache()
        val numAs = logData.filter(line => line.contains("WARRANTY")).count()
        val numBs = logData.filter(line => line.contains("b")).count()
        println("Lines with WARRANTY: %s, Lines with b: %s".format(numAs, numBs))
    }
}