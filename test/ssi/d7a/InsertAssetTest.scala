package ssi.d7a


import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.cassandra.CassandraSQLContext
import org.joda.time.DateTime;
import org.junit.Test
import org.junit.Assert._
import org.junit.Before
import org.junit.After

import com.datastax.driver.core.Cluster
import com.datastax.spark.connector.mapper.DefaultColumnMapper
import com.datastax.spark.connector._
import org.apache.spark.sql._


import scala.collection.JavaConversions._
import scala.util.control.NonFatal
import java.util.UUID

object Context {
    val cluster = Cluster.builder.addContactPoint("127.0.0.1").build
    val session = cluster.connect    
    val path = s"${System.getProperty("user.dir")}/testResources/assets/assets.csv"
    val conf = new SparkConf(true).set("spark.cassandra.connection.host", "127.0.0.1").set("spark.cassandra.connection.rpc.port", "9160")    
    val sc = new SparkContext("local", "test", conf)
    val csc: CassandraSQLContext = new CassandraSQLContext(sc)    
    val logData = sc.textFile(path).map(line => line.split("\t"))    
}

class InsertAssetTest extends java.io.Serializable {

    def $(query: String) = Context.session.execute(query)
    
    @Before
    def dropSchema {
        try $("DROP KEYSPACE testload") catch {case NonFatal(e) =>}
    }

    @After
    def closeConnection {
//        dropSchema
        Context.session.close
        Context.cluster.close
    }

    
    @Test
    def loadCSV {
        $("""CREATE KEYSPACE testload WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1 };""")
        println("created key space")
        $("CREATE TABLE testload.assets(id uuid, name text, state text, support_type text, some_string text, some_number double, amount double, amount_type text, start_date timestamp, end_date timestamp, quarter text,some_number_two int,some_number_three int,some_string_two text,some_date timestamp,some_number_four int,some_date_two timestamp,some_number_five int,some_date_three timestamp,some_number_six int,some_product_info text,some_product_info_two text,some_product_info_three text,some_product_info_four text,some_product_info_five text,some_product_info_six text, PRIMARY KEY(id,quarter,start_date,end_date));")
        $("CREATE TABLE testload.opportunities(id uuid, account_id text, amount text, close_date timestamp, is_closed boolean, created_by_id text, created_date timestamp, is_deleted boolean, description text, expected_revenue double, fiscal_year int, fiscal_quarter int, forecast_category text, forecast_category_name text, has_opportunity_line_item boolean, last_activity_date timestamp, last_modified_by_id text, last_modified_date timestamp, last_referenced_date timestamp, lastv_iewed_date timestamp, lead_source text, next_step text, currency_iso_code text, division text, name text, owner_id text, pricebook_2_id text, campaign_id text, is_private boolean, probability int, total_opportunity_quantity double, connection_received_id text, connection_sent_id text, stage_name text, system_modstamp timestamp, territory_id text, opportunity_type text, is_won boolean, PRIMARY KEY(id,fiscal_quarter,fiscal_year,name));")
        println("inserting data")

        for (line <- Context.logData) {
            try
                $("INSERT INTO testload.assets(id,name,state,support_type,some_string,some_number,amount,amount_type,start_date,end_date,quarter,some_number_two,some_number_three,some_string_two,some_date,some_number_four,some_date_two,some_number_five,some_date_three,some_number_six,some_product_info,some_product_info_two,some_product_info_three,some_product_info_four,some_product_info_five,some_product_info_six) VALUES ("+UUID.randomUUID()+",'%s','%s','%s','%s',%f,%f,'%s','%s','%s','%s',%d,%d,'%s','%s',%d,'%s',%d,'%s',%d,'%s','%s','%s','%s','%s','%s');".format(line(0),line(1),line(2),line(3),line(4).toDouble,line(5).toDouble,line(6),line(7),line(8),line(9),line(10).toInt,line(11).toInt,line(12),line(13),line(14).toInt,line(15),line(16).toInt,line(17),line(18).toInt,line(19),line(20),line(21),line(22),line(23),line(24)))
            catch {
                case NonFatal(e) => println("unable to save line: " + line.toString + "\n " + e)
            }
        }
        
        val srdd: SchemaRDD = Context.csc.sql("select * from testload.assets") // is this generating the full output
        println("count : " +  srdd.count)
        println("top row: " + srdd.first)
/*
        println(Context.rdd.columnNames.toString())
        val assetData = Context.sc.cassandraTable[Asset]("testload", "assets").select(Context.rdd.columnNames.toString()).where("quarter = ?", "FY14Q2").collect
        assetData.foreach(println)
*/
        val results = $("select * from testload.assets where quarter = 'FY14Q2' ALLOW FILTERING")
                
        for (row <- results) {
            $("INSERT INTO testload.opportunities(id, account_id, amount, close_date, is_closed, created_by_id, created_date, is_deleted, description, expected_revenue, fiscal_year, fiscal_quarter, forecast_category, forecast_category_name, has_opportunity_line_item, last_activity_date, last_modified_by_id, last_modified_date, last_referenced_date, lastv_iewed_date, lead_source, next_step, currency_iso_code, division, name, owner_id, pricebook_2_id, campaign_id, is_private, probability, total_opportunity_quantity, connection_received_id, connection_sent_id, stage_name, system_modstamp, territory_id, opportunity_type, is_won) VALUES ("+UUID.randomUUID()+",'%s','%s','%s',false,'%s','%s',false,'%s',%f,%d,%d,'%s','%s',false,'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',true,%d,%f,'%s','%s','%s','%s','%s','%s',false);"
                .format("fakeAccountId",row.getDouble("amount").toString + row.getString("amount_type"),DateTime.now,"fakeCreatedBy",DateTime.now, "fake description",row.getDouble("amount"), 2014, 2, "fake forecast category", "fake forecast category name", DateTime.now, "Jerry Lee", DateTime.now, DateTime.now,DateTime.now,"fake lead source", "next step", row.getString("amount_type"), "division", "Opportunity - " + row.getString("name"),"fake owner", "fake pricebook", "fake campaign", 99, row.getDouble("amount"), "fake connection received", "fake connection sent", "fake stage name", DateTime.now, "fake territory", "opp Type"))        
        }
        val opportunityRdd = Context.sc.cassandraTable("testload", "opportunities")
        opportunityRdd.select("id").where("fiscal_quarter = ?",2).collect.foreach(println)

        val osrdd: SchemaRDD = Context.csc.sql("select * from testload.opportunities") // is this generating the full output
        println("count : " +  osrdd.count)
        println("top row: " + osrdd.first)
        println("Done")
    }
    
}