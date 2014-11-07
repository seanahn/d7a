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
        try $("DROP KEYSPACE testload") catch {case NonFatal(e) =>}
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
        println("count : " +  srdd.count)
        assertEquals(2, rdd.count)
        
        val collection = sc.parallelize(Seq(("key3", 3), ("key4", 4)))
        collection.saveToCassandra("test", "kv", SomeColumns("key", "value"))
        println(rdd.count)
        assertEquals(4, rdd.count)
    }
    
    @Test
    def loadCSV {
        import com.datastax.spark.connector._
        import org.apache.spark.sql._
        $("""CREATE KEYSPACE testload WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };""")
        println("created key space")

        $("""CREATE TABLE testload.assets(name text,state text, supportType text,sometext text,someNumber int PRIMARY KEY,amount int,amountType text,startDate text,endDate text,quarter text,someNumberTwo int,someNumberThree int,sometextTwo text,someDate text,someNumberFour int,someDateTwo text,someNumberFive int,someDateThree text,someNumberSix int,sometextThree text,producttext text,producttextTwo text,producttextThree text,productSolution text,productType text,someNumberSeven int,sometextFour text,sometextFive text,sometextSix text,sometextSeven text,sometextEight text,sometextNine text,sometextTen text,sometextEleven text,sometextTwelve text,sometextThirteen text,sometextFourteen text,sometextFifteen text,sometextSixteen text,sometextSeventeen text,sometextEighteen text,sometextNineteen text,sometextTwenty text,sometextTwentyOne text,sometextTwentyTwo text,sometextTwentyThree text,sometextTwentyFour text,sometextTwentyFive text,sometextTwentySix text,sometextTwentySeven text,sometextTwentyEight text,sometextTwentyNine text,sometextThirty text, sometextThirtyOne text);""")

        val path = s"${System.getProperty("user.dir")}/testResources/assets/assets.csv"
        val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.rpc.port", "9160")
        val sc = new SparkContext("local", "test", conf)
        val logData = sc.textFile(path).map(line => line.split("\t"))
        val rdd = sc.cassandraTable("testload", "assets");
        println("inserting data")
        try
            logData.map(line => (line(0),
                                 line(1),
                                 line(2),
                                 line(3),
                                 line(4),
                                 line(5),
                                 line(6),
                                 line(7),
                                 line(8),
                                 line(9),
                                 line(10),
                                 line(11),
                                 line(12),
                                 line(13),
                                 line(14),
                                 line(15),
                                 line(16),
                                 line(17),
                                 line(18),
                                 line(19),
                                 line(20),
                                 line(21),
                                 line(22),
                                 line(23),
                                 line(24),
                                 line(25),
                                 line(26),
                                 line(27),
                                 line(28),
                                 line(29),
                                 line(30),
                                 line(31),
                                 line(32),
                                 line(33),
                                 line(34),
                                 line(35),
                                 line(36),
                                 line(37),
                                 line(38),
                                 line(39),
                                 line(40),
                                 line(41),
                                 line(42),
                                 line(43),
                                 line(44),
                                 line(45),
                                 line(46),
                                 line(47),
                                 line(48),
                                 line(49),
                                 line(50),
                                 line(51),
                                 line(52),
                                 line(53),
                                 line(54))).saveToCassandra(
                    "testLoad", "assets", Seq( "name",
                                            "state",
                                            "supportType", 
                                            "someString",
                                            "someNumber",
                                            "amount",
                                            "amountType",
                                            "startDate",
                                            "endDate",
                                            "quarter",
                                            "someNumberTwo",
                                            "someNumberThree",
                                            "someStringTwo",
                                            "someDate",
                                            "someNumberFour",
                                            "someDateTwo",
                                            "someNumberFive",
                                            "someDateThree",
                                            "someNumberSix",
                                            "someStringThree",
                                            "productString",
                                            "productStringTwo",
                                            "productStringThree",
                                            "productSolution",
                                            "productType",
                                            "someNumberSeven",
                                            "someStringFour",
                                            "someStringFive",
                                            "someStringSix",
                                            "someStringSeven",
                                            "someStringEight",
                                            "someStringNine",
                                            "someStringTen",
                                            "someStringEleven",
                                            "someStringTwelve",
                                            "someStringThirteen",
                                            "someStringFourteen",
                                            "someStringFifteen",
                                            "someStringSixteen",
                                            "someStringSeventeen",
                                            "someStringEighteen",
                                            "someStringNineteen",
                                            "someStringTwenty",
                                            "someStringTwentyOne",
                                            "someStringTwentyTwo",
                                            "someStringTwentyThree",
                                            "someStringTwentyFour",
                                            "someStringTwentyFive",
                                            "someStringTwentySix",
                                            "someStringTwentySeven",
                                            "someStringTwentyEight",
                                            "someStringTwentyNine",
                                            "someStringThirty",
                                            "someStringThirtyOne")
                    )
        catch {
            case NonFatal(e) =>
        }
        println("done inserting")
        val csc: CassandraSQLContext = new CassandraSQLContext(sc)
        csc.setKeyspace("testLoad")

        val srdd: SchemaRDD = csc.sql("select * from assets") // is this generating the full output
        println("count : " +  srdd.count())
    

                println(rdd.count)

    }
}