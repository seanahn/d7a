package ssi.d7a.force

import com.sforce.soap.partner.PartnerConnection
import com.sforce.ws.ConnectorConfig
import com.sforce.soap.partner.Connector
import scala.language.dynamics
import com.sforce.soap.partner.sobject.SObject

object ForceBase extends Dynamic {
    val userName = "kemiller-engdev@servicesource.com"
    val password = "password12345Q1bl8JghwYhgxpJ1l1rdNo6O"
      
    lazy val conn : PartnerConnection = {
        val config = new ConnectorConfig()
        config.setUsername(userName);
        config.setPassword(password);
        //config.setTraceMessage(true);
        Connector.newConnection(config);
    }

    def selectDynamic(typeName: String): ForceType = new ForceType(typeName)
}

class ForceType(val typeName: String) extends Dynamic {
    import ForceBase._
    
    def getByName(name: String): ForceObject = {
        val query = s"SELECT Id, Name FROM $typeName WHERE Name = '$name'"
        get(query)
    }
    
    def get(query: String): ForceObject = {
        val queryResults = conn.query(query);
        if (queryResults.getSize > 0) new ForceObject(typeName, queryResults.getRecords()(0))
        else null
    }
}

class ForceObject(val typeName: String, val obj: SObject) extends Dynamic {
    import scala.collection.JavaConversions._
    
    def selectDynamic[T](name: String): T = obj.getField(name).asInstanceOf[T]
}