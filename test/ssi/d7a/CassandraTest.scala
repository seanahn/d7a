package ssi.d7a

import org.junit.Test
import com.datastax.driver.core.Cluster
import org.junit.Before
import org.junit.After
import scala.util.control.NonFatal
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.junit.Assert._
import org.apache.spark.sql.cassandra.CassandraSQLContext
import ch.qos.logback.classic.Level
import java.util.UUID
import org.junit.BeforeClass
import org.junit.AfterClass

object CassandraTest {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
    val session = cluster.connect
    
    def $(query: String) = session.execute(query)
    
    @BeforeClass
    def dropSchema {
        try $("DROP KEYSPACE test") catch {case NonFatal(e) =>}
        
        createData
    }

    def createData {
        $("""CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };""")
        $("""CREATE TABLE test.assets (
            id uuid PRIMARY KEY, 
            title text, 
            album text, 
            artist text, 
            tags set<text>, 
            data blob 
            );""");
        $("""CREATE INDEX ON test.assets (title);""")
        $("""INSERT INTO test.assets (id, title, album, artist, tags) VALUES (
            756716f7-2e54-4715-9f00-91dcbea6cf50,
            'La Petite Tonkinoise',
            'Bye Bye Blackbird',
            'Joséphine Baker',
            {'jazz', '2013'});""")
        for(i <- 1 to 100000)
        $(s"""INSERT INTO test.assets (id, title, album, artist, tags) VALUES (
            ${UUID.randomUUID().toString()},
            'La Petite Tonkinoise-$i',
            'Bye Bye Blackbird',
            'Joséphine Baker',
            {'jazz', '2013'});""")
    }

    def setLogLevel(level: Level, names: Any*) {
        import org.slf4j.LoggerFactory._
        import ch.qos.logback.classic.Level
        import ch.qos.logback.classic.Logger

        for (name <- names) {
            if (name.isInstanceOf[Class[_]])
                getLogger(name.asInstanceOf[Class[_]]).asInstanceOf[Logger].setLevel(level)
            else
                getLogger(name.toString).asInstanceOf[Logger].setLevel(level)
        }
    }

    @AfterClass
    def closeConnection {
//        dropSchema
        session.close
        cluster.close
    }
}

class CassandraTest {
    import CassandraTest._
    
    @Test
    def test {
        setLogLevel(Level.DEBUG, "ssi")
        
//        val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.rpc.port", "9160")
//        val sc = new SparkContext("local", "test", conf)
//        import com.datastax.spark.connector._
//        val rdd = sc.cassandraTable("test", "kv")
//		println(rdd.count)
//		assertEquals(0, rdd.count)
//		println(rdd.first)

		// exercise sql now
//		import org.apache.spark.sql._
//		val csc: CassandraSQLContext = new CassandraSQLContext(sc)
//		csc.setKeyspace("test")
//        val srdd: SchemaRDD = csc.sql("select * from test.assets where title = 'A'") // is this generating the full output
////        srdd.sele
//        println(srdd.schema.fieldNames)
//        println("count : " +  srdd.count)

//        Thread.sleep(5000)
        var asset = DataService.cassandra.test.assets.get("select * from test.assets where title = 'A'")
        println(asset)

        asset = DataService.cassandra.test.assets.get("select * from test.assets where title = 'La Petite Tonkinoise'")
        println(asset)

        asset = DataService.cassandra.test.assets.get("select * from test.assets where title = 'La Petite Tonkinoise'")
        println(asset)

        asset = DataService.cassandra.test.assets.get("select * from test.assets where title = 'La Petite Tonkinoise'")
        println(asset)

        asset = DataService.cassandra.test.assets.get("select * from test.assets where title = 'La Petite Tonkinoise'")
        println(asset)
        
        val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.rpc.port", "9160")
        val sc = new SparkContext("local", "test", conf)
        import com.datastax.spark.connector._
        val assets = sc.cassandraTable("test", "assets")
        val logger = DataService.logger
        logger.debug("start cassandraTabe")
        assets.select("id").where("title = ?", "La Petite Tonkinoise").collect.foreach(println)
        logger.debug("done cassandraTabe")

        logger.debug("start cassandraTabe")
        assets.select("id").where("title = ?", "La Petite Tonkinoise").collect.foreach(println)
        logger.debug("done cassandraTabe")

        logger.debug("start cassandraTabe")
        assets.select("id").where("title = ?", "La Petite Tonkinoise").collect.foreach(println)
        logger.debug("done cassandraTabe")

        logger.debug("start cassandraTabe")
        assets.select("id").where("title = ?", "La Petite Tonkinoise").collect.foreach(println)
        logger.debug("done cassandraTabe")

//        sc.cassandraTable("test", "cars").select("id", "model").where("color = ?", "black").toArray.foreach(println)
        
        logger.debug("now cassandra native query")
        var results = session.execute("select * from test.assets where title = 'La Petite Tonkinoise'");
        println(results.one())
        logger.debug("done")

        logger.debug("now cassandra native query")
        results = session.execute("select * from test.assets where title = 'La Petite Tonkinoise' allow filtering");
        println(results.one())
        logger.debug("done")
    }
}