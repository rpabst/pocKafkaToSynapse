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
    .option("maxOffsetsPerTrigger", "5000")  //How many records I read max in every MicroBatch execution
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
   






case class SinkLoader(df: DataFrame, writeToDbFunction: DataFrame => Unit) {
  def load() {
    writeToDbFunction(df)
  }
}



def transformDf(df: DataFrame) = {
  val sinks = Seq(
    SinkLoader(df, canonicalTrackEvent), 
    SinkLoader(df, packageBatchCardRoutingInformation),
    SinkLoader(df, identifier),
    SinkLoader(df, messageHeader),
    SinkLoader(df, detailSourceInformation),
 
    SinkLoader(df, dummyFill1),
    SinkLoader(df, dummyFill2),
    SinkLoader(df, dummyFill3),
    
     SinkLoader(df, dummyFill4),
    SinkLoader(df, dummyFill5),
    SinkLoader(df, dummyFill6),
    
     SinkLoader(df, dummyFill7),
    SinkLoader(df, dummyFill8),
    SinkLoader(df, dummyFill9),
    SinkLoader(df, dummyFill10)
    
  ).par
  sinks.foreach(_.load)
}



def dummyFill1(df: DataFrame) = {
 val df_temp1 = df.withColumn("parsedXMLColumn1", parseXMLDUMMYUDF(col("value")))
   .select($"parsedXMLColumn1.dummyID".as("dummyID"))
    writeToSynapse(df_temp1, "PICKUP_TO_DELIVERY.DUMMYFORTEST_RPABST1_CS")
}

def dummyFill2(df: DataFrame) = {
 val df_temp1 = df.withColumn("parsedXMLColumn1", parseXMLDUMMYUDF(col("value")))
   .select($"parsedXMLColumn1.dummyID".as("dummyID"))
    writeToSynapse(df_temp1, "PICKUP_TO_DELIVERY.DUMMYFORTEST_RPABST2_CS")
}

def dummyFill3(df: DataFrame) = {
  val df_temp1 = df.withColumn("parsedXMLColumn1", parseXMLDUMMYUDF(col("value")))
   .select($"parsedXMLColumn1.dummyID".as("dummyID"))
    writeToSynapse(df_temp1, "PICKUP_TO_DELIVERY.DUMMYFORTEST_RPABST3_CS")
}



def dummyFill4(df: DataFrame) = {
 val df_temp1 = df.withColumn("parsedXMLColumn1", parseXMLDUMMYUDF(col("value")))
   .select($"parsedXMLColumn1.dummyID".as("dummyID"))
    writeToSynapse(df_temp1, "PICKUP_TO_DELIVERY.DUMMYFORTEST_RPABST4_CS")
}

def dummyFill5(df: DataFrame) = {
 val df_temp1 = df.withColumn("parsedXMLColumn1", parseXMLDUMMYUDF(col("value")))
   .select($"parsedXMLColumn1.dummyID".as("dummyID"))
    writeToSynapse(df_temp1, "PICKUP_TO_DELIVERY.DUMMYFORTEST_RPABST5_CS")
}

def dummyFill6(df: DataFrame) = {
  val df_temp1 = df.withColumn("parsedXMLColumn1", parseXMLDUMMYUDF(col("value")))
   .select($"parsedXMLColumn1.dummyID".as("dummyID"))
    writeToSynapse(df_temp1, "PICKUP_TO_DELIVERY.DUMMYFORTEST_RPABST6_CS")
}

def dummyFill7(df: DataFrame) = {
 val df_temp1 = df.withColumn("parsedXMLColumn1", parseXMLDUMMYUDF(col("value")))
   .select($"parsedXMLColumn1.dummyID".as("dummyID"))
    writeToSynapse(df_temp1, "PICKUP_TO_DELIVERY.DUMMYFORTEST_RPABST7_CS")
}

def dummyFill8(df: DataFrame) = {
 val df_temp1 = df.withColumn("parsedXMLColumn1", parseXMLDUMMYUDF(col("value")))
   .select($"parsedXMLColumn1.dummyID".as("dummyID"))
    writeToSynapse(df_temp1, "PICKUP_TO_DELIVERY.DUMMYFORTEST_RPABST8_CS")
}

def dummyFill9(df: DataFrame) = {
  val df_temp1 = df.withColumn("parsedXMLColumn1", parseXMLDUMMYUDF(col("value")))
   .select($"parsedXMLColumn1.dummyID".as("dummyID"))
    writeToSynapse(df_temp1, "PICKUP_TO_DELIVERY.DUMMYFORTEST_RPABST9_CS")
}


def dummyFill10(df: DataFrame) = {
  val df_temp1 = df.withColumn("parsedXMLColumn1", parseXMLDUMMYUDF(col("value")))
   .select($"parsedXMLColumn1.dummyID".as("dummyID"))
    writeToSynapse(df_temp1, "PICKUP_TO_DELIVERY.DUMMYFORTEST_RPABST10_CS")
}





def canonicalTrackEvent(df: DataFrame) = {
val df_temp = df.withColumn("parsedXMLColumn", parseXMLCTEUDF(col("value")))
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



def packageBatchCardRoutingInformation(df: DataFrame) = {
  

val df_temp1 = df.withColumn("parsedXMLColumn1", parseXMLBCIUDF(col("value")))
.select($"parsedXMLColumn1.ROUTING_INFORMATION_TYPE".as("ROUTING_INFORMATION_TYPE"), $"parsedXMLColumn1.ROUTING_INFORMATION_CODE".as("ROUTING_INFORMATION_CODE"), $"parsedXMLColumn1.COUNTRY_CODE".as("COUNTRY_CODE"),
        $"parsedXMLColumn1.ZIP_CODE".as("ZIP_CODE"), $"parsedXMLColumn1.HOUSE_NUMBER_CODE".as("HOUSE_NUMBER_CODE"), $"parsedXMLColumn1.STREET_CODE".as("STREET_CODE"),
        $"parsedXMLColumn1.PRODUCT_CODE_NATIONAL".as("PRODUCT_CODE_NATIONAL"), $"parsedXMLColumn1.PRODUCT_CODE_INTERNATIONAL".as("PRODUCT_CODE_INTERNATIONAL"), $"parsedXMLColumn1.FEATURE_CODE_INTERNATIONAL".as("FEATURE_CODE_INTERNATIONAL"))
 
 // val dfWithKey = addHashColumn(df_temp1, "BCR_ID")
     writeToSynapse(df_temp1, "PICKUP_TO_DELIVERY.PACKAGE_BATCH_CARD_ROUTING_INFORMATION_RPABST_CS")
}


def messageHeader(df: DataFrame) = {
  val df_temp = df.withColumn("parsedXMLColumn4", parseXMLMSGHEADERUDF(col("value")))
.select($"parsedXMLColumn4.MSG_HDR".as("MSG_HDR"), $"parsedXMLColumn4.MSG_HDR_CREATION_TS".as("MSG_HDR_CREATION_TS")  ,  $"parsedXMLColumn4.MSG_HDR_DEVICE_ID".as("MSG_HDR_DEVICE_ID"), $"parsedXMLColumn4.MSG_HDR_DEVICE_NAME".as("MSG_HDR_DEVICE_NAME"),
        $"parsedXMLColumn4.MSG_HDR_DEVICE_TYPE".as("MSG_HDR_DEVICE_TYPE"), $"parsedXMLColumn4.MSG_HDR_DS_DSTID".as("MSG_HDR_DS_DSTID"), $"parsedXMLColumn4.MSG_HDR_DS_NAME".as("MSG_HDR_DS_NAME"),
        $"parsedXMLColumn4.MSG_HDR_DS_RECEIVING_TS".as("MSG_HDR_DS_RECEIVING_TS"), $"parsedXMLColumn4.MSG_HDR_DS_VERSION".as("MSG_HDR_DS_VERSION"), $"parsedXMLColumn4.MSG_HDR_EXPORT_FILE".as("MSG_HDR_EXPORT_FILE"),
        $"parsedXMLColumn4.MSG_HDR_ID".as("MSG_HDR_ID"), $"parsedXMLColumn4.MSG_HDR_ID_TYPE".as("MSG_HDR_ID_TYPE"), $"parsedXMLColumn4.MSG_HDR_IS_TEST".as("MSG_HDR_IS_TEST"),
        $"parsedXMLColumn4.MSG_HDR_MSG_FORMAT_TYPE".as("MSG_HDR_MSG_FORMAT_TYPE"), $"parsedXMLColumn4.MSG_HDR_MSG_FORMAT_VER".as("MSG_HDR_MSG_FORMAT_VER"))
   writeToSynapse(df_temp, "PICKUP_TO_DELIVERY.MESSAGE_HEADER_RPABST_CS")
}


        
def identifier(df: DataFrame) = {
  val df_temp = df.withColumn("parsedXMLColumn3", parseXMLIDENTUDF(col("value")))
.select($"parsedXMLColumn3.IDENTIFIER_TYPE".as("IDENTIFIER_TYPE"), $"parsedXMLColumn3.IDENTIFIER_VALUE".as("IDENTIFIER_VALUE"), $"parsedXMLColumn3.IDENTIFIER_STATUS".as("IDENTIFIER_STATUS"),
        $"parsedXMLColumn3.MAIN_IDENTIFIER_FLAG".as("MAIN_IDENTIFIER_FLAG"), $"parsedXMLColumn3.SHIPMENT_IDENTIFIER_FLAG".as("SHIPMENT_IDENTIFIER_FLAG"))
 
//  val dfWithKey = addHashColumn(df_temp, "IDC_ID")
  writeToSynapse(df_temp, "PICKUP_TO_DELIVERY.IDENTIFIER_RPABST_CS")
}


def detailSourceInformation(df: DataFrame) = {
  val df_temp = df.withColumn("parsedXMLColumn2", parseXMLDSIUDF(col("value")))
    .select($"parsedXMLColumn2.DETAIL_SOURCE_INFORMATION_TYPE".as("DETAIL_SOURCE_INFORMATION_TYPE"), $"parsedXMLColumn2.DETAIL_SOURCE_INFORMATION_VALUE".as("DETAIL_SOURCE_INFORMATION_VALUE"))
   
 // val dfWithKey = addHashColumn(df_temp, "DSI_ID")
  writeToSynapse(df_temp, "PICKUP_TO_DELIVERY.DETAIL_SOURCE_INFORMATION_RPABST_CS")
}






val parseXMLCTEUDF = udf((xmlRawInput:String) => {
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
spark.udf.register("parseXMLCTEUDF", parseXMLCTEUDF)



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
     

/*
PARSE XML BCI

**/


val parseXMLBCIUDF = udf((xmlRawInput:String) => {
    val xmlStructure = XML.loadString(xmlRawInput)
    val BatchCardRoutingInformationSubtree = xmlStructure \ "packageBatchCard" \"partnerInformation" 
  
     val ROUTING_INFORMATION_TYPE = (BatchCardRoutingInformationSubtree \ "role").text
     val ROUTING_INFORMATION_CODE = (BatchCardRoutingInformationSubtree \ "role").text
     val COUNTRY_CODE = (BatchCardRoutingInformationSubtree \ "location" \ "address" \ "countryCode" ).text
     val ZIP_CODE = (BatchCardRoutingInformationSubtree \ "location" \ "address" \ "zip" ).text
     val HOUSE_NUMBER_CODE = (BatchCardRoutingInformationSubtree \ "location" \ "address" \ "houseNumber" ).text
     val STREET_CODE = (BatchCardRoutingInformationSubtree \ "location" \ "address" \ "street" ).text
     val PRODUCT_CODE_NATIONAL = (BatchCardRoutingInformationSubtree \ "location" \ "address" \ "city" ).text
     val PRODUCT_CODE_INTERNATIONAL = (BatchCardRoutingInformationSubtree \ "location" \ "address" \ "countryCode" ).text
     val FEATURE_CODE_INTERNATIONAL = (BatchCardRoutingInformationSubtree \ "location" \ "address" \ "countryCode" ).text
  
     packageBatchCardInformationDTO(ROUTING_INFORMATION_TYPE, ROUTING_INFORMATION_CODE,  COUNTRY_CODE, ZIP_CODE, HOUSE_NUMBER_CODE, STREET_CODE,
                                  PRODUCT_CODE_NATIONAL, PRODUCT_CODE_INTERNATIONAL, FEATURE_CODE_INTERNATIONAL)
  })
spark.udf.register("parseXMLBCIUDF", parseXMLBCIUDF)





case class packageBatchCardInformationDTO(ROUTING_INFORMATION_TYPE: String, ROUTING_INFORMATION_CODE: String, COUNTRY_CODE: String, ZIP_CODE: String, HOUSE_NUMBER_CODE: String, STREET_CODE: String,
                                  PRODUCT_CODE_NATIONAL: String, PRODUCT_CODE_INTERNATIONAL: String, FEATURE_CODE_INTERNATIONAL: String)
                                 
val packageBatchCardInformationSchema = new StructType()
    .add("ROUTING_INFORMATION_TYPE", StringType)
    .add("ROUTING_INFORMATION_CODE", StringType)
    .add("COUNTRY_CODE", StringType)
    .add("ZIP_CODE", StringType)
    .add("HOUSE_NUMBER_CODE", StringType)
    .add("STREET_CODE", StringType)
    .add("PRODUCT_CODE_NATIONAL", StringType)
    .add("PRODUCT_CODE_INTERNATIONAL", StringType)
    .add("FEATURE_CODE_INTERNATIONAL", StringType)
   





val parseXMLDUMMYUDF = udf((xmlRawInput:String) => {
     val xmlStructure = XML.loadString(xmlRawInput)
     val BatchCardRoutingInformationSubtree = xmlStructure \ "packageBatchCard" \"partnerInformation" 
     val dummyID = (BatchCardRoutingInformationSubtree \ "role").text
    
  
     parseXMLDUMMYDTO(dummyID)
  })
spark.udf.register("parseXMLDUMMYUDF", parseXMLDUMMYUDF)


case class parseXMLDUMMYDTO(dummyID: String)
val parseXMLDUMMYSchema = new StructType()
    .add("dummyID", StringType)
   
   





/*
PARSE XML DETAIL SOURCE INFO

**/
val parseXMLDSIUDF = udf((xmlRawInput:String) => {
    val xmlStructure = XML.loadString(xmlRawInput)
    val detailSourceSubtree = xmlStructure \ "packageBatchCard" \"events" \"event" \ "messageHeader" \ "triggeringMessage" \ "dataSource "
     val DETAIL_SOURCE_INFORMATION_TYPE = (detailSourceSubtree \  "detailSourceInformation" \ "@type").text
     val DETAIL_SOURCE_INFORMATION_VALUE = (detailSourceSubtree \  "detailSourceInformation" \ "value").text
     
     detailSourceInfoDTO(DETAIL_SOURCE_INFORMATION_TYPE, DETAIL_SOURCE_INFORMATION_VALUE)
  })
spark.udf.register("parseXMLDSIUDF", parseXMLDSIUDF)

case class detailSourceInfoDTO(DETAIL_SOURCE_INFORMATION_TYPE: String,       DETAIL_SOURCE_INFORMATION_VALUE: String)
val detailSourceInfo = new StructType()
    .add("DETAIL_SOURCE_INFORMATION_TYPE", StringType)
    .add("DETAIL_SOURCE_INFORMATION_VALUE", StringType)



/*
PARSE XML identifier

**/
val parseXMLIDENTUDF = udf((xmlRawInput:String) => {
    val xmlStructure = XML.loadString(xmlRawInput)
    val identifierSubtree = xmlStructure \ "packageBatchCard" \"events" \"event"
    val IDENTIFIER_TYPE = (identifierSubtree \  "packageInformation" \ "packageIdentifier" \ "@type").text
    val IDENTIFIER_VALUE = (identifierSubtree  \  "packageInformation" \ "packageIdentifier" \ "@value").text
    val IDENTIFIER_STATUS = (identifierSubtree  \  "packageInformation" \ "packageIdentifier" \ "@status").text
    val MAIN_IDENTIFIER_FLAG = "1" //(identifierSubtree  \  "packageInformation" \ "packageIdentifier").text
    val SHIPMENT_IDENTIFIER_FLAG = "1" // (identifierSubtree  \  "shipmentInformation").text
  
     
     identifierDTO(IDENTIFIER_TYPE, IDENTIFIER_VALUE, IDENTIFIER_STATUS, MAIN_IDENTIFIER_FLAG, SHIPMENT_IDENTIFIER_FLAG)
  })
spark.udf.register("parseXMLIDENTUDF", parseXMLIDENTUDF)

case class identifierDTO(IDENTIFIER_TYPE: String,       IDENTIFIER_VALUE: String, IDENTIFIER_STATUS: String, MAIN_IDENTIFIER_FLAG: String, SHIPMENT_IDENTIFIER_FLAG:String  )
val identifierSchema = new StructType()
    .add("IDENTIFIER_TYPE", StringType)
    .add("IDENTIFIER_VALUE", StringType)
    .add("IDENTIFIER_STATUS", StringType)
    .add("MAIN_IDENTIFIER_FLAG", StringType)
    .add("SHIPMENT_IDENTIFIER_FLAG", StringType)




/*
PARSE XML MessageHeader

**/
val parseXMLMSGHEADERUDF = udf((xmlRawInput:String) => {
  val xmlStructure = XML.loadString(xmlRawInput)
  val messageHeaderSubtree = xmlStructure \ "packageBatchCard" \"events" \"event" \ "messageHeader"
  val MSG_HDR = ( messageHeaderSubtree).text
  val MSG_HDR_CREATION_TS = ( messageHeaderSubtree \ "creationTimestamp").text
  val MSG_HDR_DEVICE_ID = ( messageHeaderSubtree \ "dataSource" \ "device" \ "identifier").text
  val MSG_HDR_DEVICE_NAME = ( messageHeaderSubtree \ "dataSource" \ "device" \ "name").text
  val MSG_HDR_DEVICE_TYPE = ( messageHeaderSubtree \ "dataSource" \ "device" \ "type").text
    
  val MSG_HDR_DS_DSTID = "1" // ( messageHeaderSubtree \ "dataSource" \ "dstId" ).text
  val MSG_HDR_DS_NAME = ( messageHeaderSubtree \ "dataSource" \ "name").text
  val MSG_HDR_DS_RECEIVING_TS = ( messageHeaderSubtree \ "dataSource" \ "receivingTimestamp").text
  val MSG_HDR_DS_VERSION = ( messageHeaderSubtree \ "dataSource" \ "version").text
  val MSG_HDR_EXPORT_FILE = ( messageHeaderSubtree \ "exportFile").text
    
  val MSG_HDR_ID = ( messageHeaderSubtree \ "identifier" \ "@type" ).text
  val MSG_HDR_ID_TYPE = ( messageHeaderSubtree  \ "identifier" \ "@type" ).text
  val MSG_HDR_IS_TEST = ( messageHeaderSubtree  \ "messageFormat" \ "type").text
  val MSG_HDR_MSG_FORMAT_TYPE = ( messageHeaderSubtree  \ "messageFormat" \ "type").text
  val MSG_HDR_MSG_FORMAT_VER = ( messageHeaderSubtree  \ "messageFormat" \ "type").text
    
     messageHeaderDTO(MSG_HDR, MSG_HDR_CREATION_TS, MSG_HDR_DEVICE_ID, MSG_HDR_DEVICE_NAME, MSG_HDR_DEVICE_TYPE,
                           MSG_HDR_DS_DSTID, MSG_HDR_DS_NAME, MSG_HDR_DS_RECEIVING_TS, MSG_HDR_DS_VERSION, MSG_HDR_EXPORT_FILE,
                           MSG_HDR_ID, MSG_HDR_ID_TYPE, MSG_HDR_IS_TEST, MSG_HDR_MSG_FORMAT_TYPE, MSG_HDR_MSG_FORMAT_VER)
  })
spark.udf.register("parseXMLMSGHEADERUDF", parseXMLMSGHEADERUDF)

case class messageHeaderDTO(MSG_HDR: String, MSG_HDR_CREATION_TS: String, MSG_HDR_DEVICE_ID: String, MSG_HDR_DEVICE_NAME: String, MSG_HDR_DEVICE_TYPE:String,
                           MSG_HDR_DS_DSTID: String, MSG_HDR_DS_NAME: String, MSG_HDR_DS_RECEIVING_TS: String, MSG_HDR_DS_VERSION: String, MSG_HDR_EXPORT_FILE:String,
                           MSG_HDR_ID: String, MSG_HDR_ID_TYPE: String, MSG_HDR_IS_TEST: String, MSG_HDR_MSG_FORMAT_TYPE: String, MSG_HDR_MSG_FORMAT_VER:String)
val messageHeader = new StructType()
    .add("MSG_HDR", StringType)
    .add("MSG_HDR_CREATION_TS", StringType)
    .add("MSG_HDR_DEVICE_ID", StringType)
    .add("MSG_HDR_DEVICE_NAME", StringType)
    .add("MSG_HDR_DEVICE_TYPE", StringType)

    .add("MSG_HDR_DS_DSTID", StringType)
    .add("MSG_HDR_DS_NAME", StringType)
    .add("MSG_HDR_DS_RECEIVING_TS", StringType)
    .add("MSG_HDR_DS_VERSION", StringType)
    .add("MSG_HDR_EXPORT_FILE", StringType)

    .add("MSG_HDR_ID", StringType)
    .add("MSG_HDR_ID_TYPE", StringType)
    .add("MSG_HDR_IS_TEST", StringType)
    .add("MSG_HDR_MSG_FORMAT_TYPE", StringType)
    .add("MSG_HDR_MSG_FORMAT_VER", StringType)







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



