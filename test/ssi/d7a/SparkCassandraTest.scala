package ssi.d7a

import org.junit.Test
import com.datastax.driver.core.Cluster
import org.junit.Before
import org.junit.After
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.util.control.NonFatal

class SparkCassandraTest {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
    val session = cluster.connect
    
    def $(query: String) = session.execute(query)
    
    @Before
    def dropSchema {
        try $("DROP KEYSPACE test") catch {case NonFatal(e) =>}
    }

    @After
    def closeConnection {
//        dropSchema
        session.close
        cluster.close
    }

    @Test
    def test {
        import com.datastax.spark.connector._
        $("""CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };""")
        $("""CREATE TABLE test.kv(key text PRIMARY KEY, value int);""")
        $("""INSERT INTO test.kv(key, value) VALUES ('key1', 1)""")
        $("""INSERT INTO test.kv(key, value) VALUES ('key2', 2)""")
//$("CREATE KEYSPACE test WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};")
//        $("""CREATE KEYSPACE test WITH REPLICATION = {'class' : 'NetworkTopologyStrategy', 'Analytics' : 1};""")
        $("""CREATE TABLE test.words (word text PRIMARY KEY, count int);""")
        $("""INSERT INTO test.words (word, count) VALUES ('foo', 10);""")
        $("""INSERT INTO test.words (word, count) VALUES ('bar', 20);""")
        
        val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.rpc.port", "9160")
//        val conf = new SparkConf(true).set("cassandra.connection.host", "127.0.0.1")
        val sc = new SparkContext("local", "test", conf)
//        sc.addJar("/opt/play-2.2.1/repository/cache/com.datastax.spark/spark-cassandra-connector_2.10/jars/spark-cassandra-connector_2.10-1.1.0-beta1.jar")
//        val csc = com.datastax.spark.connector.toSparkContextFunctions(sc)
        val rdd = sc.cassandraTable("test", "kv")
		println(rdd.count)
//		println(rdd.first)
//		println(rdd.map(_.getInt("value")).sum)  
    }
}