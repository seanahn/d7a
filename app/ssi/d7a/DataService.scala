package ssi.d7a

import scala.language.dynamics
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SchemaRDD
import org.apache.spark.sql.Row
import play.Logger

object DataService {
    val logger = Logger.of(getClass)
    
    def cassandra = CassandraBase
}

object CassandraBase extends Dynamic {
    val keySpacesByName = scala.collection.mutable.Map[String, CassandraKeyspace]()
    
    def selectDynamic(keySpaceName: String): CassandraKeyspace = keySpacesByName.getOrElse(keySpaceName, {
        val keyspace = new CassandraKeyspace(keySpaceName)
        keySpacesByName += keySpaceName -> keyspace
        keyspace
    })
}

class CassandraKeyspace(val keySpaceName: String) extends Dynamic {
    import DataService._

    lazy val context: CassandraSQLContext = {
        logger.debug("Creating context...")
        val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.rpc.port", "9160")
        val sc = new SparkContext("local", keySpaceName, conf)
		val csc: CassandraSQLContext = new CassandraSQLContext(sc)
		csc.setKeyspace(keySpaceName)
		csc
    }
  
    def selectDynamic(typeName: String): CassandraType = new CassandraType(context, typeName)
}

class CassandraType(val context: CassandraSQLContext, val typeName: String) extends Dynamic {
    import DataService._

    def getByName(name: String): CassandraObject = {
        val query = s"select * from ${context.getKeyspace}.$typeName where name = '$name'"
        get(query)
    }
    
    def get(query: String): CassandraObject = {
        logger.debug(query)
        val srdd: SchemaRDD = context.sql(query)
        logger.debug("query returned")
        if (srdd.count > 0) new CassandraObject(typeName, srdd.schema.fieldNames, srdd.first)
        else null
    }
}

class CassandraObject(val typeName: String, val fieldNames: Seq[String], val obj: Row) extends Dynamic {
    lazy val indexesByName = {
        val map = scala.collection.mutable.Map[String, Int]()
        for((field, index) <- fieldNames.zipWithIndex) map += field -> index
        scala.collection.immutable.Map(map.toList: _*)
    }
    
    def selectDynamic[T](name: String): T = {
        indexesByName.get(name) match {
          case Some(i) => obj(i).asInstanceOf[T]
          case None => null.asInstanceOf[T]
        }
    }
}