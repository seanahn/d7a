package ssi.d7a

import org.junit.Assert._

import org.junit.Test
import java.io._
import java.util._

import com.sforce.async._
import com.sforce.soap.partner.PartnerConnection
import com.sforce.ws.ConnectionException
import com.sforce.ws.ConnectorConfig

class ForceConnectTest {

    @Test
    def test {
        val path = System.getProperty("user.dir") + "/testResources/opportunities/opportunities.csv"
        val connection = getBulkConnection("kemiller-engdev@servicesource.com", "password12345Q1bl8JghwYhgxpJ1l1rdNo6O")
        val job = createJob("Opportunity", connection)
        val batchInfoList = createBatchesFromCSVFile(connection, job, s"${System.getProperty("user.dir")}/testResources/opportunities/opportunities.csv")
        closeJob(connection, job.getId())
        awaitCompletion(connection, job, batchInfoList)
        assertEquals(1, checkResults(connection, job, batchInfoList))
    }

    /**
     * Create the BulkConnection used to call Bulk API operations.
     */
    def getBulkConnection(userName: String, password: String): BulkConnection = {
        val partnerConfig = new ConnectorConfig()
        partnerConfig.setUsername(userName)
        partnerConfig.setPassword(password)
        partnerConfig.setAuthEndpoint("https://login.salesforce.com/services/Soap/u/32.0")
        new PartnerConnection(partnerConfig)
        
        val config = new ConnectorConfig()
        config.setSessionId(partnerConfig.getSessionId())
        val soapEndpoint = partnerConfig.getServiceEndpoint()
        val restEndpoint = s"${soapEndpoint.substring(0, soapEndpoint.indexOf("Soap/"))}async/32.0"
        config.setRestEndpoint(restEndpoint)
        // This should only be false when doing debugging.
        config.setCompression(true)
        // Set this to true to see HTTP requests and responses on stdout
        config.setTraceMessage(false)
        new BulkConnection(config)
    }

    def createJob(sobjectType: String, connection: BulkConnection): JobInfo = {
        var job = new JobInfo()
        job.setObject(sobjectType)
        job.setOperation(OperationEnum.insert)
        job.setContentType(ContentType.CSV)
        job = connection.createJob(job)
        System.out.println(job)
        job
    }

    def createBatchesFromCSVFile(connection: BulkConnection, jobInfo: JobInfo, csvFileName: String): java.util.List[BatchInfo] = {
        val batchInfos = new java.util.ArrayList[BatchInfo]()
        val rdr = new BufferedReader(new InputStreamReader(new FileInputStream(csvFileName)))
        // read the CSV header row
        val headerBytes = (rdr.readLine() + "\n").getBytes("UTF-8")
        val headerBytesLength = headerBytes.length
        val tmpFile = File.createTempFile("bulkAPIInsert", ".csv")

        // Split the CSV file into multiple batches
        try {
            var tmpOut = new FileOutputStream(tmpFile)
            val maxBytesPerBatch = 10000000 // 10 million bytes per batch
            val maxRowsPerBatch = 10000 // 10 thousand rows per batch
            var currentBytes = 0
            var currentLines = 0
            var nextLine: String = rdr.readLine()
            while (nextLine != null) {
                val bytes = (nextLine + "\n").getBytes("UTF-8")
                // Create a new batch when our batch size limit is reached
                if (currentBytes + bytes.length > maxBytesPerBatch || currentLines > maxRowsPerBatch) {
                    createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo)
                    currentBytes = 0
                    currentLines = 0
                }
                if (currentBytes == 0) {
                    tmpOut = new FileOutputStream(tmpFile)
                    tmpOut.write(headerBytes)
                    currentBytes = headerBytesLength
                    currentLines = 1
                }
                tmpOut.write(bytes)
                currentBytes += bytes.length
                currentLines += 1
                nextLine = rdr.readLine()
            }
            // Finished processing all rows
            // Create a final batch for any remaining data
            if (currentLines > 1) {
                createBatch(tmpOut, tmpFile, batchInfos, connection, jobInfo)
            }
        } finally {
            tmpFile.delete()
        }
        return batchInfos
    }

    def createBatch(tmpOut: FileOutputStream, tmpFile: File, batchInfos:java.util.List[BatchInfo],
            connection: BulkConnection, jobInfo: JobInfo) = {
        tmpOut.flush()
        tmpOut.close()
        val tmpInputStream = new FileInputStream(tmpFile)
        try {
            val batchInfo = connection.createBatchFromStream(jobInfo, tmpInputStream)
            System.out.println(batchInfo)
            batchInfos.add(batchInfo)

        } finally {
            tmpInputStream.close()
        }
    }

    def closeJob(connection: BulkConnection, jobId: String) {
        val job = new JobInfo()
        job.setId(jobId)
        job.setState(JobStateEnum.Closed)
        connection.updateJob(job)
    }

    /**
     * Wait for a job to complete by polling the Bulk API.
     * 
     * @param connection
     *            BulkConnection used to check results.
     * @param job
     *            The job awaiting completion.
     * @param batchInfoList
     *            List of batches for this job.
     * @throws AsyncApiException
     */
    def awaitCompletion(connection: BulkConnection, job: JobInfo, batchInfoList:java.util.List[BatchInfo]) {
        var sleepTime = 0L
        val incomplete = new java.util.HashSet[String]()
        import scala.collection.JavaConversions._
        for (bi <- batchInfoList) {
            incomplete.add(bi.getId())
        }
        while (!incomplete.isEmpty()) {
            try {
                Thread.sleep(sleepTime)
            } catch {
              case e: InterruptedException =>
            }
            System.out.println("Awaiting results..." + incomplete.size())
            sleepTime = 10000L
            val statusList = connection.getBatchInfoList(job.getId()).getBatchInfo()
            for (b <- statusList) {
                if (b.getState() == BatchStateEnum.Completed || b.getState() == BatchStateEnum.Failed) {
                    if (incomplete.remove(b.getId())) {
                        System.out.println("BATCH STATUS:\n" + b)
                    }
                }
            }
        }
    }

    /**
     * Gets the results of the operation and checks for errors.
     */
    def checkResults(connection : BulkConnection, job : JobInfo, batchInfoList : java.util.List[BatchInfo]): Int = {
        // batchInfoList was populated when batches were created and submitted
        import scala.collection.JavaConversions._
        
        var total = 0
        for (b <- batchInfoList) {
            val rdr = new CSVReader(connection.getBatchResultStream(job.getId(), b.getId()))
            val resultHeader = rdr.nextRecord()
            val resultCols = resultHeader.size()

            var row: java.util.List[String] = rdr.nextRecord()
            while (row != null) {
                val resultInfo = new java.util.HashMap[String, String]()
                for (i <- 0 to resultCols -1) {
                    resultInfo.put(resultHeader.get(i), row.get(i))
                }
                val success = java.lang.Boolean.valueOf(resultInfo.get("Success"))
                val created = java.lang.Boolean.valueOf(resultInfo.get("Created"))
                val id = resultInfo.get("Id")
                val error = resultInfo.get("Error")
                if (success && created) {
                    System.out.println("Created row with id " + id)
                    total += 1
                } else if (!success) {
                    System.out.println("Failed with error: " + error)
                }
                row = rdr.nextRecord()
            }
        }
        total
    }
}
