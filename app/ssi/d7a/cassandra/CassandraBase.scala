package ssi.d7a.cassandra

import scala.language.dynamics
import play.Logger
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.Row
import scala.reflect.runtime.universe._
import java.util.Date
import java.util.UUID
import java.text.SimpleDateFormat
import com.datastax.driver.core.ColumnDefinitions
import com.datastax.driver.core.exceptions.InvalidQueryException
import scala.collection.JavaConversions._

object CassandraBase extends Dynamic {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
    val session = cluster.connect

    val logger = Logger.of(getClass)

    var schemaless = false
    
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

    var _columns: Map[String, String] = null

    def columns: Map[String, String] = {
        if(_columns == null)
	        _columns = selectOrElse({
	            val sql = s"select * from $keySpaceName.$typeName limit 1"
	            logger.debug(sql)
		        val res = session.execute(sql)
		        println(res.getColumnDefinitions().size())
		        val map = scala.collection.mutable.Map[String, String]()
		        for(cd <- res.getColumnDefinitions().asList) {
		            map += cd.getName -> cd.getType.toString
		        }
		        Map(map.toSeq: _*)
	        }, Map.empty)
        
	    println(_columns)
        _columns
    }
    
    def spawn = new CassandraObject(typeName, null)
    
    def get(query: String): CassandraObject = {
        logger.debug(query)
        selectOrElse({
	        val results = session.execute(query)
	        results.one() match {
	          case null => null
	          case r: Row => new CassandraObject(typeName, r)
	        }
        }, null)
    }
    
    def selectOrElse[T](f: => T, g: => T): T = {
        if(!schemaless) return f
        
        try f catch { 
            case iqe: InvalidQueryException => 
                if(schemaless && iqe.getMessage().contains("unconfigured columnfamily")) g.asInstanceOf[T] 
                else throw iqe
        }
    }

    def fetch(query: String): Iterator[CassandraObject] = {
        logger.debug(query)
        selectOrElse({
	        val results = session.execute(query).iterator
	        return new Iterator[CassandraObject] {
	            def hasNext = results.hasNext
	            
	            def next = new CassandraObject(typeName, results.next)
	        }
        }, Seq.empty.toIterator)
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
    
    def adjustSchema(co: CassandraObject) {
        val newColumns = co.fields.keys.toBuffer.diff(columns.keys.toBuffer)
        if(newColumns.isEmpty) return
        
        if(columns.isEmpty) { // new table
            if(newColumns.contains("id")) newColumns -= "id"
            val cols = newColumns.map(column => s"$column ${guessType(co.get(column))}").mkString(",\n\t")
            val sql = s"CREATE TABLE $keySpaceName.$typeName (\n\tid uuid PRIMARY KEY,\n\t$cols\n);"
            logger.info(sql)
            session.execute(sql)
        } else { // add columns
            newColumns.map(column => {
                val sql = s"ALTER TABLE $keySpaceName.$typeName ADD $column ${guessType(co.get(column))}"
                logger.info(sql)
                session.execute(sql)
            })
        }
        _columns = null
    }
    
    def put(co: CassandraObject): CassandraObject = {
        adjustSchema(co)
        
        val sql = if(co.obj == null) { // new object insert or replace
            if( co.id == null ) co.id = UUID.randomUUID // new object insert
            
            val names = co.fields.keys.mkString(", ")
            val values = co.fields.keys.map(key => (key, co.fields(key))).map(e => 
                serialize(columns(e._1.toLowerCase), e._2)
            ).mkString(", ")
            s"insert into $keySpaceName.$typeName($names) values($values)"
        } else { // merge
            val kvs = co.fields.keys.map(key => {
                val v = serialize(columns(key.toLowerCase), co.fields(key))
                s"$key = $v"
            }).mkString(", ")
            s"update $keySpaceName.$typeName set $kvs where id = ${co.id}"
        }

        logger.debug(sql)
        session.execute(sql)
        getById(co.id.asInstanceOf[UUID])
    }
    
    def +=(co: CassandraObject) = put(co)
    
    def getByName(name: String): CassandraObject = {
        val query = s"select * from $keySpaceName.$typeName where name = '$name'"
        get(query)
    }
    
    def getById(id: UUID): CassandraObject = {
        val query = s"select * from $keySpaceName.$typeName where id = $id"
        get(query)
    }
    
    def apply(id: UUID) = getById(id)

    def serialize(cassandraType: String, value: Any): String = {
        logger.debug(s"serializing $value to $cassandraType...")
        cassandraType match {
            case "uuid" => value.toString
            case "timestamp" => "'" + new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ").format(value.asInstanceOf[Date]) + "'"
            case _ => s"'${value.toString}'"
        }
    }
    
    def guessType(value: Any): String = {
        value match {
          case _: Int => "int"
          case _: Date => "timestamp"
          case _: UUID => "uuid"
          case _ => "varchar"
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