package ssi.d7a

import org.junit.Test
import org.junit.BeforeClass
import com.datastax.driver.core.Cluster
import com.datastax.driver.core.utils.UUIDs
import scala.collection.mutable.Buffer
import scala.collection.JavaConversions._
import org.junit.Before
import scala.util.control.NonFatal

class DataServiceTest {
    lazy val session = Cluster.builder.addContactPoint("127.0.0.1").build.connect
    def $(query: String) = session.execute(query)
    
    lazy val schema = {
    }
    
    var lastSong: String = null
    
    @Before
    def setupData {
        try $("DROP KEYSPACE simplex") catch {
          case NonFatal(e) =>
        }
        
        $("CREATE KEYSPACE simplex WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};")
        $("""CREATE TABLE simplex.songs (
            id uuid PRIMARY KEY, 
            title text, 
            album uuid, 
            artist text, 
            tags set<text>, 
            data blob 
            );""");
        $("""CREATE TABLE simplex.albums (
            id uuid PRIMARY KEY,
            title text,
            artist text,
            tags set<text>
            );""")

        for(album <- 1 to 100) {
            val songs = Buffer[String]()
            val albumId = UUIDs.random.toString
            for(song <- 1 to 14) {
                val id = UUIDs.random.toString
                lastSong = id
                songs += id
                $(s"""INSERT INTO simplex.songs (id, title, album, artist, tags) VALUES (
                    $id,
                    'title-$album-$song',
                    $albumId,
                    'artist-$album',
                    {'jazz', '2013'});""")
            }
        
            $(s"""INSERT INTO simplex.albums (id, title, artist, tags) VALUES (
                $albumId,
                'title-$album',
                'artist-$album',
                {'jazz', '2013'});""")
        }
    }
    
    @Test
    def testFetch {
        DataService.fetch
        val results = session.execute(s"SELECT * FROM simplex.songs WHERE id = $lastSong;");
		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
		       "-------------------------------+-----------------------+--------------------"));
		for (row <- results) {
		    System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
		    row.getUUID("album"),  row.getString("artist")));
		}
		System.out.println();
    }
}