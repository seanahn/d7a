package ssi.d7a.cassandra

import scala.language.dynamics
import play.Logger
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Row
import scala.reflect.runtime.universe._
import java.util.Date
import java.util.UUID
import scala.swing.Table
import java.text.SimpleDateFormat
import com.datastax.driver.core.ColumnDefinitions

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
        val cassandraType = new CassandraType(keySpaceName, typeName)
        typesByName += typeName -> cassandraType
        cassandraType
    })
}

class CassandraType(val keySpaceName: String, val typeName: String) {
    import CassandraBase._

    lazy val columns: ColumnDefinitions = {
        val res = session.execute(s"select * from $keySpaceName.$typeName limit 1")
        res.getColumnDefinitions()
    }
    
    def spawn = new CassandraObject(typeName, null)
    
    def getByName(name: String): CassandraObject = {
        val query = s"select * from $keySpaceName.$typeName where name = '$name'"
        get(query)
    }
    
    def getById(id: UUID): CassandraObject = {
        val query = s"select * from $keySpaceName.$typeName where id = $id"
        get(query)
    }
    
    def apply(id: UUID) = getById(id)

    def get(query: String): CassandraObject = {
        logger.debug(query)
        val results = session.execute(query)
        results.one() match {
          case null => null
          case r: Row => new CassandraObject(typeName, r)
        }
    }

    def put(tuples: (String, Any)*): CassandraObject = {
        val co = new CassandraObject(typeName, null)
        co.fields ++= tuples.toMap
        put(co)
    }
    
    def +=(tuples: (String, Any)*) = {
        val co = new CassandraObject(typeName, null)
        co.fields ++= tuples.toMap
        put(co)
    }
        
    def put(co: CassandraObject): CassandraObject = {
        val sql = if(co.obj == null) { // new object insert or replace
            if( co.id == null ) co.id = UUID.randomUUID // new object insert
            
            val names = co.fields.keys.mkString(", ")
            val values = co.fields.keys.map(key => (key, co.fields(key))).map(e => serialize(columns.getType(e._1).toString, e._2)).mkString(", ")
            s"insert into $keySpaceName.$typeName($names) values($values)"
        } else { // merge
            val kvs = co.fields.keys.map(key => {
              val v = serialize(columns.getType(key).toString, co.fields(key)) 
              s"$key = $v"
            }).mkString(", ")
            s"update $keySpaceName.$typeName set $kvs where id = ${co.id}"
        }

        logger.debug(sql)
        session.execute(sql)
        getById(co.id.asInstanceOf[UUID])
    }
    
    def +=(co: CassandraObject) = put(co)
    
    def serialize(cassandraType: String, value: Any): String = {
        logger.debug(s"serializing $value to $cassandraType...")
        cassandraType match {
            case "uuid" => value.toString
            case "timestamp" => "'" + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(value.asInstanceOf[Date]) + "'"
            case _ => s"'${value.toString}'"
        }
    }
}

class CassandraObject(val typeName: String, val obj: Row) extends Dynamic {
    import CassandraBase._

    val fields = scala.collection.mutable.Map[String, Any]()
    
    def get[T](name: String)(implicit tag: TypeTag[T]): T = {
        fields.getOrElse(name, {
            if(obj == null) return null.asInstanceOf[T]
            
	        val srcType = obj.getColumnDefinitions().getType(name)
            logger.debug(s"deserializing $typeName.$name of ${srcType.toString} type...")
	        val srcVal = srcType.toString match {
	            case "uuid" => obj.getUUID(name).asInstanceOf[Any]
	            case "int" => obj.getInt(name).asInstanceOf[Any]
	            case "timestamp" => obj.getDate(name).asInstanceOf[Any]
	            case "varchar" => obj.getString(name).asInstanceOf[Any]
	        }
            srcVal
        }).asInstanceOf[T]
//        if (tag.tpe == typeOf[Nothing]) return srcVal.asInstanceOf[T]
        
//        val cls = runtimeMirror(getClass.getClassLoader).runtimeClass(tag.tpe.typeSymbol.asClass)
//        if(classOf[Integer].isAssignableFrom(cls)) obj.getBool(name).asInstanceOf[T]
//        else if(classOf[Array[Byte]].isAssignableFrom(cls)) obj.getBytes(name).asInstanceOf[T]
//        else if(classOf[Date].isAssignableFrom(cls)) obj.getDate(name).asInstanceOf[T]
//        else if(classOf[Double].isAssignableFrom(cls)) obj.getDecimal(name).asInstanceOf[T]
//        else if(classOf[Integer].isAssignableFrom(cls)) obj.getInt(name).asInstanceOf[T]
//        else throw new RuntimeException(s"Scala type: $cls not supported!")
    }
    
    def selectDynamic[T](name: String)(implicit tag: TypeTag[T]): T = get(name)
    
    def set(name: String)(value: Any) = fields += name -> value

    def updateDynamic(name: String)(value: Any) = set(name)(value)
}