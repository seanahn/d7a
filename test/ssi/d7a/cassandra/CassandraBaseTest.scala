package ssi.d7a.cassandra

import com.datastax.driver.core.Cluster
import org.junit.BeforeClass
import scala.util.control.NonFatal
import org.junit.AfterClass
import org.junit.Before
import ch.qos.logback.classic.Level
import java.util.UUID
import org.junit.Test
import org.junit.Assert._
import java.text.SimpleDateFormat
import java.util.Date

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
            serial int,
            album text, 
            artist text,
            released timestamp,
            tags set<text>, 
            data blob 
            );""");
        $("""CREATE INDEX ON test.assets (title);""")
        $("""INSERT INTO test.assets (id, title, serial, album, artist, released, tags) VALUES (
            756716f7-2e54-4715-9f00-91dcbea6cf50,
            'La Petite Tonkinoise',
            2,
            'Bye Bye Blackbird',
            'Joséphine Baker',
            '2014-01-02',
            {'jazz', '2013'});""")
//        for(i <- 1 to 100000)
//        $(s"""INSERT INTO test.assets (id, title, album, artist, tags) VALUES (
//            ${UUID.randomUUID().toString()},
//            'La Petite Tonkinoise-$i',
//            'Bye Bye Blackbird',
//            'Joséphine Baker',
//            {'jazz', '2013'});""")
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

        CassandraBase.logger.debug("getting table")
        val assets = CassandraBase.test.assets
        CassandraBase.logger.debug("getting row")
        var asset = assets.get("select * from test.assets where title = 'La Petite Tonkinoise'")
        CassandraBase.logger.debug(asset.toString)

        CassandraBase.logger.debug("getting row")
        asset = assets.get("select * from test.assets where title = 'La Petite Tonkinoise'")
        CassandraBase.logger.debug(asset.toString)
        
        val dump = (v: Any) => println(v.toString + " of " + v.getClass)
        
        dump(asset.id)
        assertEquals(UUID.fromString("756716f7-2e54-4715-9f00-91dcbea6cf50"), asset.id)
        
        dump(asset.title)
        assertEquals("La Petite Tonkinoise", asset.title)

        dump(asset.serial)
        assertEquals(new Integer(2), asset.serial)

        dump(asset.released)
        assertEquals(new SimpleDateFormat("yyyy-MM-dd").parse("2014-01-02"), asset.released)

        CassandraBase.logger.debug("getting row")
        asset = assets.get("select * from test.assets where title = 'DOES NOT EXIST'")
        assertNull(asset)
        
        // insert using a map
        val now = System.currentTimeMillis()
        var newAsset: CassandraObject = assets += ("id" -> null, "title" -> "new", "released" -> new Date(now))
        assertEquals("new", newAsset.title)
        assertEquals(now / 1000 * 1000, newAsset.released.asInstanceOf[Date].getTime())
        
        // update a field
        newAsset.title = "updated"
        newAsset = assets.put(newAsset)
        assertEquals("updated", newAsset.title)
        assertEquals(now / 1000 * 1000, newAsset.released.asInstanceOf[Date].getTime())

        // insert using setters
        newAsset = assets.spawn
        newAsset.title = "spawned"
        newAsset.released = new Date(now)
        newAsset = assets += newAsset
        assertEquals("spawned", newAsset.title)
        assertEquals(now / 1000 * 1000, newAsset.released.asInstanceOf[Date].getTime())
    }
}