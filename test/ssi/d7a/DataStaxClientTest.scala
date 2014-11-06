package ssi.d7a

import org.junit.Test
import com.datastax.driver.core.Cluster
import scala.collection.JavaConversions._
import org.junit.After
import org.junit.Before
import scala.util.control.NonFatal

class DataStaxClientTest {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
    val session = cluster.connect
    
    def $(query: String) = session.execute(query)
    
    @Before
    def dropSchema {
        try $("DROP KEYSPACE simplex") catch{case NonFatal(e) =>}
    }

    @After
    def closeConnection {
        dropSchema
        session.close
        cluster.close
    }
    
    @Test
    def test {
        val metadata = cluster.getMetadata
        printf("Connected to cluster: %s\n", metadata.getClusterName)
        for ( host <- metadata.getAllHosts ) {
            printf("Datatacenter: %s; Host: %s; Rack: %s\n",
               host.getDatacenter, host.getAddress, host.getRack)
        }
        
        $("CREATE KEYSPACE simplex WITH replication = {'class':'SimpleStrategy', 'replication_factor':3};")
        $("""CREATE TABLE simplex.songs (
            id uuid PRIMARY KEY, 
            title text, 
            album text, 
            artist text, 
            tags set<text>, 
            data blob 
            );""");
        $("""CREATE TABLE simplex.playlists (
            id uuid,
            title text,
            album text,  
            artist text,
            song_id uuid,
            PRIMARY KEY (id, title, album, artist)
            );""")
    
        $("""INSERT INTO simplex.songs (id, title, album, artist, tags) VALUES (
            756716f7-2e54-4715-9f00-91dcbea6cf50,
            'La Petite Tonkinoise',
            'Bye Bye Blackbird',
            'Joséphine Baker',
            {'jazz', '2013'});""")
        $("""INSERT INTO simplex.playlists (id, song_id, title, album, artist) VALUES (
            2cc9ccb7-6221-4ccb-8387-f22b6a1b354d,
            756716f7-2e54-4715-9f00-91dcbea6cf50,
            'La Petite Tonkinoise',
            'Bye Bye Blackbird',
            'Joséphine Baker');""")

        val results = session.execute("SELECT * FROM simplex.playlists WHERE id = 2cc9ccb7-6221-4ccb-8387-f22b6a1b354d;");
		System.out.println(String.format("%-30s\t%-20s\t%-20s\n%s", "title", "album", "artist",
		       "-------------------------------+-----------------------+--------------------"));
		for (row <- results) {
		    System.out.println(String.format("%-30s\t%-20s\t%-20s", row.getString("title"),
		    row.getString("album"),  row.getString("artist")));
		}
		System.out.println();
    }
}