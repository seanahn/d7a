package ssi.d7a

import org.junit.Test
import org.junit.Assert._
import com.datastax.driver.core.Cluster
import org.junit.Before
import org.junit.After
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.control.NonFatal
import org.apache.spark.sql.cassandra.CassandraSQLContext

object Spark {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
    val session = cluster.connect
    val path = s"${System.getProperty("user.dir")}/testResources/assets/assets.csv"
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.rpc.port", "9160")
    val sc = new SparkContext("local", "test", conf)
    val csc: CassandraSQLContext = new CassandraSQLContext(sc)
    val logData = sc.textFile(path).map(line => line.split("\t"))
}

class InsertAssetTest extends java.io.Serializable {

    def $(query: String) = Spark.session.execute(query)
    
    @Before
    def dropSchema {
        try $("DROP KEYSPACE testload") catch {case NonFatal(e) =>}
    }

    @After
    def closeConnection {
//        dropSchema
        Spark.session.close
        Spark.cluster.close
    }

    
    @Test
    def loadCSV {
        import com.datastax.spark.connector._
        import org.apache.spark.sql._
        $("""CREATE KEYSPACE testload WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };""")
        println("created key space")
        $("CREATE TABLE testload.assets(name text,state text, supportType text,someString text,someNumber text PRIMARY KEY,amount text,amountType text,startDate text,endDate text,quarter text);")
        println("inserting data")


        for (line <- Spark.logData) {
            $("INSERT INTO testload.assets(name,state,supportType,someString,someNumber,amount,amountType,startDate,endDate,quarter) "+" VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');".format(line(0),line(1),line(2),line(3),line(4),line(5),line(6),line(7),line(8),line(9)))
        }
        
        val srdd: SchemaRDD = Spark.csc.sql("select * from testload.assets") // is this generating the full output
        println("count : " +  srdd.count)
        println("Done")
    }
}