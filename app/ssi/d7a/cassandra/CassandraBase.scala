package ssi.d7a.cassandra

import scala.language.dynamics
import play.Logger
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Row
import scala.reflect.runtime.universe._
import java.util.Date

object CassandraBase extends Dynamic {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
    val session = cluster.connect

    val logger = Logger.of(getClass)

    val keySpacesByName = scala.collection.mutable.Map[String, CassandraKeyspace]()
    
    def selectDynamic(keySpaceName: String): CassandraKeyspace = keySpacesByName.getOrElse(keySpaceName, {
        val keyspace = new CassandraKeyspace(keySpaceName)
        keySpacesByName += keySpaceName -> keyspace
        keyspace
    })
}

class CassandraKeyspace(val keySpaceName: String) extends Dynamic {
    import CassandraBase._

    val typesByName = scala.collection.mutable.Map[String, CassandraType]()

    def selectDynamic(typeName: String): CassandraType = typesByName.getOrElse(typeName, {
        val cassandraType = new CassandraType(typeName, typeName)
        typesByName += keySpaceName -> cassandraType
        cassandraType
    })
}

class CassandraType(val keySpaceName: String, val typeName: String) extends Dynamic {
    import CassandraBase._

    def getByName(name: String): CassandraObject = {
        val query = s"select * from $keySpaceName.$typeName where name = '$name'"
        get(query)
    }
    
    def get(query: String): CassandraObject = {
        logger.debug(query)
        val results = session.execute(query)
        results.one() match {
          case null => null
          case r: Row => new CassandraObject(typeName, r)
        }
    }
}

class CassandraObject(val typeName: String, val obj: Row) extends Dynamic {
    def selectDynamic[T](name: String)(implicit tag: TypeTag[T]): T = {
        val srcType = obj.getColumnDefinitions().getType(name)
        println(srcType.asJavaClass)
        val srcVal = srcType.toString match {
            case "uuid" => obj.getUUID(name).asInstanceOf[Any]
            case "int" => obj.getInt(name).asInstanceOf[Any]
            case "timestamp" => obj.getDate(name).asInstanceOf[Any]
            case "varchar" => obj.getString(name).asInstanceOf[Any]
        }
        return srcVal.asInstanceOf[T]
//        if (tag.tpe == typeOf[Nothing]) return srcVal.asInstanceOf[T]
        
//        val cls = runtimeMirror(getClass.getClassLoader).runtimeClass(tag.tpe.typeSymbol.asClass)
//        if(classOf[Integer].isAssignableFrom(cls)) obj.getBool(name).asInstanceOf[T]
//        else if(classOf[Array[Byte]].isAssignableFrom(cls)) obj.getBytes(name).asInstanceOf[T]
//        else if(classOf[Date].isAssignableFrom(cls)) obj.getDate(name).asInstanceOf[T]
//        else if(classOf[Double].isAssignableFrom(cls)) obj.getDecimal(name).asInstanceOf[T]
//        else if(classOf[Integer].isAssignableFrom(cls)) obj.getInt(name).asInstanceOf[T]
//        else throw new RuntimeException(s"Scala type: $cls not supported!")
    }
}