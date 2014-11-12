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
        $("""CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };""")
        $("""CREATE TABLE test.kv(key text PRIMARY KEY, value int);""")
        $("""INSERT INTO test.kv(key, value) VALUES ('key1', 1)""")
        $("""INSERT INTO test.kv(key, value) VALUES ('key2', 2)""")
        
        val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.rpc.port", "9160")
        val sc = new SparkContext("local", "test", conf)
        import com.datastax.spark.connector._
        val rdd = sc.cassandraTable("test", "kv")
		println(rdd.count)
		assertEquals(2, rdd.count)
		println(rdd.first)

		// exercise sql now
		import org.apache.spark.sql._
		val csc: CassandraSQLContext = new CassandraSQLContext(sc)
		csc.setKeyspace("tets")
        val srdd: SchemaRDD = csc.sql("select * from test.kv") // is this generating the full output
        println(srdd.schema.fieldNames)
        println("count : " +  srdd.count)
		assertEquals(2, rdd.count)
        
        val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
        collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))

		println(rdd.count)
		assertEquals(4, rdd.count)
    }

}