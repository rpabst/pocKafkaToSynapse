import spark.sqlContext.implicits._
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.util.concurrent.Executors
import org.apache.spark.sql.functions.udf 
import org.apache.spark.sql.types._
import scala.xml._


spark.conf.set("spark.streaming.backpressure.enabled","true")

//the maximum rate (in messages per second) at which each Kafka partition will be read
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", 25000)

//number of messages per second per partition.
//spark.conf.set("spark.streaming.backpressure.pid.minRate", 500)




val inputFromKafkaStream = 
  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-odspoc.westeurope.azurecontainer.io:9092")
   .option("subscribe", "dhlpocboundstreamp10")     
//  .option("subscribe", "dhlpocboundstreamp10,dhlpocboundstreamp101,dhlpocboundstreamp102")     
    .option("startingOffsets", "latest")  
    .option("minPartitions", "40")  
 //   .option("maxOffsetsPerTrigger", "1000000")  //How many records I read max in every MicroBatch execution
    .option("maxOffsetsPerTrigger", "80000")  //How many records I read max in every MicroBatch execution
    .option("failOnDataLoss", "true")
    .load()



val inputFromKafkaStreamCast = inputFromKafkaStream.selectExpr("CAST(value AS STRING)")
val inputFromKafkaStreamCastWithPk = addHashColumn(inputFromKafkaStreamCast, "PK")

handleStream(inputFromKafkaStreamCastWithPk)

def handleStream(df: DataFrame) = {
  df
    .writeStream
    .outputMode("append")
    .foreachBatch { (df: DataFrame, batchId: Long) =>
      transformDf(df)
    }
    .start()
}  
   


def canonicalTrackEvent(df: DataFrame) = {
  val df_temp = df.withColumn("parsedXMLColumn", parseXMLUDF(col("value")))
.select($"parsedXMLColumn.EVENT_ID".as("EVENT_ID"), $"parsedXMLColumn.CREATION_TIMESTAMP".as("CREATION_TIMESTAMP"), $"parsedXMLColumn.HASH_VALUE".as("HASH_VALUE"),
        $"parsedXMLColumn.PRIMARY_CRITERIA".as("PRIMARY_CRITERIA"), $"parsedXMLColumn.IS_OBSERVATION_FLAG".as("IS_OBSERVATION_FLAG"), $"parsedXMLColumn.EVENT_TYPE".as("EVENT_TYPE"),
        $"parsedXMLColumn.MESSAGE_SENDER".as("MESSAGE_SENDER"), $"parsedXMLColumn.MESSAGE_RECIPIENT".as("MESSAGE_RECIPIENT"), $"parsedXMLColumn.MESSAGE_CREATION_TS".as("MESSAGE_CREATION_TS"),
        $"parsedXMLColumn.MESSAGE_ADDITIONAL_INFORMATION".as("MESSAGE_ADDITIONAL_INFORMATION"), $"parsedXMLColumn.MESSAGE_IS_TEST_FLAG".as("MESSAGE_IS_TEST_FLAG"), $"parsedXMLColumn.DATA_SOURCE_NAME".as("DATA_SOURCE_NAME"),
        $"parsedXMLColumn.DATA_SOURCE_VERSION".as("DATA_SOURCE_VERSION"), $"parsedXMLColumn.TRIGGERING_MESSAGE_CREATION_TS".as("TRIGGERING_MESSAGE_CREATION_TS"), $"parsedXMLColumn.TRIGGERING_MESSAGE_IS_TEST_FLAG".as("TRIGGERING_MESSAGE_IS_TEST_FLAG"),
        $"parsedXMLColumn.IDENTIFIER_SOURCE_1".as("IDENTIFIER_SOURCE_1"), $"parsedXMLColumn.IDENTIFIER_SOURCE_2".as("IDENTIFIER_SOURCE_2"), $"parsedXMLColumn.IDENTIFIER_SOURCE_3".as("IDENTIFIER_SOURCE_3"),
        $"parsedXMLColumn.IDENTIFIER_SOURCE_4".as("IDENTIFIER_SOURCE_4"), $"parsedXMLColumn.IDENTIFIER_SOURCE_5".as("IDENTIFIER_SOURCE_5"), $"parsedXMLColumn.MESSAGE_FORMAT_TYPE".as("MESSAGE_FORMAT_TYPE"),
        $"parsedXMLColumn.MESSAGE_FORMAT_NAME".as("MESSAGE_FORMAT_NAME"), $"parsedXMLColumn.MESSAGE_FORMAT_VERSION".as("MESSAGE_FORMAT_VERSION"), $"parsedXMLColumn.DSTID".as("DSTID"),
        $"parsedXMLColumn.DETAIL_SOURCE_INFORMATION".as("DETAIL_SOURCE_INFORMATION"), $"parsedXMLColumn.TARGET_TYPE_SOURCE".as("TARGET_TYPE_SOURCE"), $"parsedXMLColumn.ACTUAL_WEIGHT".as("ACTUAL_WEIGHT"),
        $"parsedXMLColumn.ACTUAL_WEIGHT_UNIT".as("ACTUAL_WEIGHT_UNIT"), $"parsedXMLColumn.ACTUAL_WIDTH".as("ACTUAL_WIDTH"), $"parsedXMLColumn.ACTUAL_WIDTH_UNIT".as("ACTUAL_WIDTH_UNIT"),
        $"parsedXMLColumn.ACTUAL_HEIGHT".as("ACTUAL_HEIGHT"), $"parsedXMLColumn.ACTUAL_HEIGHT_UNIT".as("ACTUAL_HEIGHT_UNIT"), $"parsedXMLColumn.ACTUAL_LENGTH".as("ACTUAL_LENGTH"),
        $"parsedXMLColumn.ACTUAL_LENGTH_UNIT".as("ACTUAL_LENGTH_UNIT"), $"parsedXMLColumn.ACTUAL_VOLUME".as("ACTUAL_VOLUME"))

   writeToSynapse(df_temp, "PICKUP_TO_DELIVERY.CANONICAL_TRACK_EVENT_TEST_RPABST_CS")
}


val parseXMLUDF = udf((xmlRawInput:String) => {
    val xmlStructure = XML.loadString(xmlRawInput)
    val canonicalEventSubtree = xmlStructure \ "packageBatchCard" \"events" \"event"
    val EVENT_ID = (canonicalEventSubtree \ "id").text
    val CREATION_TIMESTAMP = (canonicalEventSubtree	 \ "creationTimestamp").text
    val HASH_VALUE = (canonicalEventSubtree \ "messageHeader" \ "sender").text
    val PRIMARY_CRITERIA = (canonicalEventSubtree \ "primaryCriteriaEventClassification").text
    val IS_OBSERVATION_FLAG = (canonicalEventSubtree \ "isObservation").text
    val EVENT_TYPE = (canonicalEventSubtree \ "type").text
    val MESSAGE_SENDER = (canonicalEventSubtree \ "messageHeader" \ "sender").text
    val MESSAGE_RECIPIENT = (canonicalEventSubtree \ "messageHeader" \ "recipient").text
    val MESSAGE_CREATION_TS = (canonicalEventSubtree \ "messageHeader" \ "creationTimestamp").text
    val MESSAGE_ADDITIONAL_INFORMATION = (canonicalEventSubtree \ "messageHeader" \ "processDescription").text
    val MESSAGE_IS_TEST_FLAG = (canonicalEventSubtree \ "messageHeader" \ "isTest").text
    val DATA_SOURCE_NAME = (canonicalEventSubtree \ "messageHeader" \ "triggeringMessage" \	"dataSource " \ "dstId").text
    val DATA_SOURCE_VERSION = (canonicalEventSubtree \ "messageHeader" \ "triggeringMessage" \	"dataSource " \ "metaLoadId").text
    val TRIGGERING_MESSAGE_CREATION_TS = (canonicalEventSubtree \ "messageHeader" \ "triggeringMessage" \ "creationTimestamp").text
    val TRIGGERING_MESSAGE_IS_TEST_FLAG = (canonicalEventSubtree \ "messageHeader" \ "triggeringMessage" \  "isTest").text
    val IDENTIFIER_SOURCE_1 = (canonicalEventSubtree \ "messageHeader" \ "triggeringMessage" \	"dataSource " \ "detailSourceInformation" \ "0" \ "@_type").text
    val IDENTIFIER_SOURCE_2 = (canonicalEventSubtree \ "messageHeader" \ "triggeringMessage" \	"dataSource " \ "detailSourceInformation" \ "1" \ "@_type").text
    val IDENTIFIER_SOURCE_3 = (canonicalEventSubtree \ "messageHeader" \ "triggeringMessage" \	"dataSource " \ "detailSourceInformation" \ "2" \ "@_type").text
    val IDENTIFIER_SOURCE_4 = (canonicalEventSubtree \ "messageHeader" \ "triggeringMessage" \	"dataSource " \ "detailSourceInformation" \ "3" \ "@_type").text
    val IDENTIFIER_SOURCE_5 = (canonicalEventSubtree \ "messageHeader" \ "triggeringMessage" \	"dataSource " \ "detailSourceInformation" \ "4" \ "@_type").text
    val MESSAGE_FORMAT_TYPE = (canonicalEventSubtree \ "messageHeader" \ "messageFormat"\ "type").text
    val MESSAGE_FORMAT_NAME = (canonicalEventSubtree \ "messageHeader" \ "messageFormat"\ "name").text
    val MESSAGE_FORMAT_VERSION = (canonicalEventSubtree \ "messageHeader" \ "messageFormat"\ "version").text
    val DSTID = (canonicalEventSubtree \ "messageHeader" \ "dataSource" \ "dstId").text
    val DETAIL_SOURCE_INFORMATION = (canonicalEventSubtree\ "messageHeader" \ "dataSource" \ "software" \ "type"	).text
    val TARGET_TYPE_SOURCE = (canonicalEventSubtree\ "messageHeader" \ "dataSource" \ "software" \ "name"	).text
    val ACTUAL_WEIGHT = (canonicalEventSubtree \  "packageInformation" \ "measure" \ "weight" \ "@value").text
    val ACTUAL_WEIGHT_UNIT =(canonicalEventSubtree \  "packageInformation" \ "measure" \ "weightunit" \ "@value").text
    val ACTUAL_WIDTH =(canonicalEventSubtree \  "packageInformation" \ "measure" \ "width" \ "@value").text
    val ACTUAL_WIDTH_UNIT = (canonicalEventSubtree \  "packageInformation" \ "measure" \ "width_unit" \ "@value").text
    val ACTUAL_HEIGHT = (canonicalEventSubtree \  "packageInformation" \ "measure" \ "height" \ "@value").text
    val ACTUAL_HEIGHT_UNIT = (canonicalEventSubtree \  "packageInformation" \ "measure" \ "height_unit" \ "@value").text
    val ACTUAL_LENGTH = (canonicalEventSubtree \  "packageInformation" \ "measure" \ "length" \ "@value").text
    val ACTUAL_LENGTH_UNIT = (canonicalEventSubtree \  "packageInformation" \ "measure" \ "length_unit" \ "@value").text
    val ACTUAL_VOLUME = (canonicalEventSubtree \  "packageInformation" \ "measure" \ "volume" \ "@value").text
  
  
  
    canonicalTrackEventDTO(EVENT_ID, CREATION_TIMESTAMP, HASH_VALUE,PRIMARY_CRITERIA, IS_OBSERVATION_FLAG, EVENT_TYPE,
                                  MESSAGE_SENDER, MESSAGE_RECIPIENT, MESSAGE_CREATION_TS, MESSAGE_ADDITIONAL_INFORMATION, MESSAGE_IS_TEST_FLAG, DATA_SOURCE_NAME,
                                  DATA_SOURCE_VERSION, TRIGGERING_MESSAGE_CREATION_TS, TRIGGERING_MESSAGE_IS_TEST_FLAG, IDENTIFIER_SOURCE_1, IDENTIFIER_SOURCE_2, IDENTIFIER_SOURCE_3,
                                  IDENTIFIER_SOURCE_4, IDENTIFIER_SOURCE_5, MESSAGE_FORMAT_TYPE, MESSAGE_FORMAT_NAME, MESSAGE_FORMAT_VERSION, DSTID,
                                  DETAIL_SOURCE_INFORMATION, TARGET_TYPE_SOURCE, ACTUAL_WEIGHT, ACTUAL_WEIGHT_UNIT, ACTUAL_WIDTH,
                                  ACTUAL_WIDTH_UNIT, ACTUAL_HEIGHT, ACTUAL_HEIGHT_UNIT, ACTUAL_LENGTH,ACTUAL_LENGTH_UNIT,ACTUAL_VOLUME)
  })
spark.udf.register("parseXMLUDF", parseXMLUDF)



case class canonicalTrackEventDTO(EVENT_ID: String, CREATION_TIMESTAMP: String, HASH_VALUE: String, PRIMARY_CRITERIA: String, IS_OBSERVATION_FLAG: String, EVENT_TYPE: String,
                                  MESSAGE_SENDER: String, MESSAGE_RECIPIENT: String, MESSAGE_CREATION_TS: String, MESSAGE_ADDITIONAL_INFORMATION: String, MESSAGE_IS_TEST_FLAG: String, DATA_SOURCE_NAME: String,
                                  DATA_SOURCE_VERSION: String, TRIGGERING_MESSAGE_CREATION_TS: String, TRIGGERING_MESSAGE_IS_TEST_FLAG: String, IDENTIFIER_SOURCE_1: String, IDENTIFIER_SOURCE_2: String, IDENTIFIER_SOURCE_3: String,
                                  IDENTIFIER_SOURCE_4: String, IDENTIFIER_SOURCE_5: String, MESSAGE_FORMAT_TYPE: String, MESSAGE_FORMAT_NAME: String, MESSAGE_FORMAT_VERSION: String, DSTID: String,
                                  DETAIL_SOURCE_INFORMATION: String, TARGET_TYPE_SOURCE: String, ACTUAL_WEIGHT: String, ACTUAL_WEIGHT_UNIT: String, ACTUAL_WIDTH: String,
                                  ACTUAL_WIDTH_UNIT: String, ACTUAL_HEIGHT: String, ACTUAL_HEIGHT_UNIT: String, ACTUAL_LENGTH: String,ACTUAL_LENGTH_UNIT: String,ACTUAL_VOLUME: String)
                                 
val canonicalTrackEventSchema = new StructType()
    .add("EVENT_ID", StringType)
    .add("CREATION_TIMESTAMP", StringType)
    .add("HASH_VALUE", StringType)
    .add("PRIMARY_CRITERIA", StringType)
    .add("IS_OBSERVATION_FLAG", StringType)
    .add("EVENT_TYPE", StringType)
    .add("MESSAGE_SENDER", StringType)
    .add("MESSAGE_RECIPIENT", StringType)
    .add("MESSAGE_CREATION_TS", StringType)
    .add("MESSAGE_ADDITIONAL_INFORMATION", StringType)
    .add("MESSAGE_IS_TEST_FLAG", StringType)
    .add("DATA_SOURCE_NAME", StringType)
    .add("DATA_SOURCE_VERSION", StringType)
    .add("TRIGGERING_MESSAGE_CREATION_TS", StringType)
    .add("TRIGGERING_MESSAGE_IS_TEST_FLAG", StringType)
    .add("IDENTIFIER_SOURCE_1", StringType)
    .add("IDENTIFIER_SOURCE_2", StringType)
    .add("IDENTIFIER_SOURCE_3", StringType)
    .add("IDENTIFIER_SOURCE_4", StringType)
    .add("IDENTIFIER_SOURCE_5", StringType)
    .add("MESSAGE_FORMAT_TYPE", StringType)
    .add("MESSAGE_FORMAT_NAME", StringType)
    .add("MESSAGE_FORMAT_VERSION", StringType)
    .add("DSTID", StringType)
    .add("DETAIL_SOURCE_INFORMATION", StringType)
    .add("TARGET_TYPE_SOURCE", StringType)
    .add("ACTUAL_WEIGHT", StringType)
    .add("ACTUAL_WEIGHT_UNIT", StringType)
    .add("ACTUAL_WIDTH", StringType)
    .add("ACTUAL_WIDTH_UNIT", StringType)
    .add("ACTUAL_HEIGHT", StringType)
    .add("ACTUAL_HEIGHT_UNIT", StringType)
    .add("ACTUAL_LENGTH", StringType)
    .add("ACTUAL_LENGTH_UNIT", StringType)
    .add("ACTUAL_VOLUME", StringType)
     





case class SinkLoader(df: DataFrame, writeToDbFunction: DataFrame => Unit) {
  def load() {
    writeToDbFunction(df)
  }
}



def transformDf(df: DataFrame) = {
  val sinks = Seq(
    SinkLoader(df, canonicalTrackEvent) 
  ).par
  sinks.foreach(_.load)
}



def writeToSQLDB(df: DataFrame, dbtable: String) = {
  val url = "jdbc:sqlserver://odspocdb.database.windows.net:1433;databaseName=ODSPOCDB"
  val user = "odspocdbservadm"
  val password = "pM6ccktz3keuM5m" 
  df.write
      .format("jdbc")
      .mode("append")
      .option("url", url)
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password", password)
      .option("encrypt", "true")
      .option("hostNameInCertificate", "*.database.windows.net")
      .save()  
}






def addHashColumn(df_tmp: DataFrame, newColName: String): DataFrame = {
  val columns = df_tmp.columns.map(col)
  
  df_tmp.withColumn(newColName, xxhash64(columns:_*))
} 



def writeToSynapse(df: DataFrame, dbtable: String) = {
  val url = "jdbc:sqlserver://odspocsynapse.sql.azuresynapse.net:1433;database=odspocdedpool"
  val user = "odspocdbservadm"
  val password = "pM6ccktz3keuM5m" 
  val datasource_name = "KafkaDataSource"
  df.repartition(1)
  .write
      .format("com.microsoft.sqlserver.jdbc.spark")
      .mode("append")
      .option("url", url)
      .option("dbtable", dbtable)
      .option("user", user)
      .option("password", password)
      .option("encrypt", "true")
      .option("mssqlIsolationLevel", "READ_UNCOMMITTED")
      .option("tabLock", "true")
      .option("hostNameInCertificate", "*.database.windows.net")
      .option("batchsize", 1000000)
      .save()  
}



