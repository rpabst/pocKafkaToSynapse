import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.util.concurrent.Executors



spark.conf.set("spark.streaming.backpressure.enabled","true")

//the maximum rate (in messages per second) at which each Kafka partition will be read
spark.conf.set("spark.streaming.kafka.maxRatePerPartition", 15000)

//number of messages per second per partition.
spark.conf.set("spark.streaming.backpressure.pid.minRate", 500)




val inputFromKafkaStream = 
  spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka-odspoc.westeurope.azurecontainer.io:9092")
    .option("subscribe", "dhlpocboundstreamp10")     
    .option("startingOffsets", "latest")  
    .option("minPartitions", "10")  
    .option("maxOffsetsPerTrigger", "5000")  //How many records I read max in one Batch execution
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
/*    SinkLoader(df, additionalStatusCode), */
    SinkLoader(df, canonicalTrackEventSmall) 
  //  SinkLoader(df, canonicalTrackEventNotificationHeader),
  // SinkLoader(df, computedRouteNode),
  //  SinkLoader(df, detailSourceInformation) 
/*    SinkLoader(df, identifier),
    SinkLoader(df, messageHeader),
    SinkLoader(df, notificationType),
    SinkLoader(df, packageBatchCard),
    SinkLoader(df, packageBatchCardPartnerInformation),
    SinkLoader(df, packageBatchCardRoutingInformation),
    SinkLoader(df, packageBatchCardValueAddedService),
    SinkLoader(df, partnerInformation),
    SinkLoader(df, partnerRoutingInformation),
    SinkLoader(df, sollwegEdge) */
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




def canonicalTrackEventSmall(df: DataFrame) = {
  val df_temp = df.selectExpr(EVENT_ID,CREATION_TIMESTAMP,HASH_VALUE,PRIMARY_CRITERIA,SECONDARY_CRITERIA,FAKE_EVENT_FLAG,IS_OBSERVATION_FLAG,ADDITIONAL_INFORMATION,PARENT_CONTAINER_TYPE,PARENT_CONTAINER_IDENTIFIER_TYPE,PARENT_CONTAINER_IDENTIFIER_VALUE,PARENT_ADDITIONAL_CONTAINER_IDENTIFIER_TYPE,PARENT_ADDITIONAL_CONTAINER_IDENTIFIER_VALUE,PARENT_CONTAINER_TIMESTAMP,EVENT_TYPE,MESSAGE_SENDER,MESSAGE_RECIPIENT,MESSAGE_CREATION_TS,MESSAGE_IS_TEST_FLAG,MESSAGE_ADDITIONAL_INFORMATION,PMES_SEQUENCE_NUMBER,TRIGGERING_MESSAGE_SENDER,TRIGGERING_MESSAGE_RECIPIENT,TRIGGERING_MESSAGE_CREATION_TS,TRIGGERING_MESSAGE_IS_TEST_FLAG,DSTID,DATA_SOURCE_NAME,DATA_SOURCE_VERSION,DETAIL_SOURCE_INFORMATION,MESSAGE_FORMAT_NAME,MESSAGE_FORMAT_VERSION,DANGEROUS_GOODS_CODE,DISTRIBUTION_PRODUCT_CODE,HANDLING_CODE,ACTUAL_WEIGHT,ACTUAL_WEIGHT_UNIT,ACTUAL_WIDTH,ACTUAL_WIDTH_UNIT, "PK as CTE_ID")

   writeToSQLDB(df_temp, "PICKUP_TO_DELIVERY.CANONICAL_TRACK_EVENT")
}






val EVENT_ID = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/id/text()')) as EVENT_ID"
val CREATION_TIMESTAMP = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/creationTimestamp/text()')) as CREATION_TIMESTAMP"
val HASH_VALUE = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/hashValue/text()')) as HASH_VALUE"
val PRIMARY_CRITERIA = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/primaryCriteriaEventClassification/text()')) as PRIMARY_CRITERIA"
val SECONDARY_CRITERIA = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/secondaryCriteriaEventClassification/text()')) as SECONDARY_CRITERIA"
val FAKE_EVENT_FLAG = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/fakeEvent/text()')) as FAKE_EVENT_FLAG"
val IS_OBSERVATION_FLAG = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/isObservation/text()')) as IS_OBSERVATION_FLAG"
val ADDITIONAL_INFORMATION = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/additionalInformation/text()')) as ADDITIONAL_INFORMATION"
val PARENT_CONTAINER_TYPE = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/parentContainerInformation/containerType/text()')) as PARENT_CONTAINER_TYPE"
val PARENT_CONTAINER_IDENTIFIER_TYPE = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/additionalInformation/parentContainerInformation/containerIdentifier/@type')) as PARENT_CONTAINER_IDENTIFIER_TYPE"
val PARENT_CONTAINER_IDENTIFIER_VALUE = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/additionalInformation/parentContainerInformation/containerIdentifer/@value')) as PARENT_CONTAINER_IDENTIFIER_VALUE"
val PARENT_ADDITIONAL_CONTAINER_IDENTIFIER_TYPE = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/additionalInformation/parentContainerInformation/additionalContainerIdentifier/@type')) as PARENT_ADDITIONAL_CONTAINER_IDENTIFIER_TYPE"
val PARENT_ADDITIONAL_CONTAINER_IDENTIFIER_VALUE = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/additionalInformation/parentContainerInformation/additionaContainerIdentifer/@value')) as PARENT_ADDITIONAL_CONTAINER_IDENTIFIER_VALUE"
val PARENT_CONTAINER_TIMESTAMP = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/additionalInformation/parentContainerInformation/timestamp/text()')) as PARENT_CONTAINER_TIMESTAMP"
val EVENT_TYPE = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/type/text()')) as EVENT_TYPE"
val MESSAGE_SENDER = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/triggeringMessage/sender/text()')) as MESSAGE_SENDER"
val MESSAGE_RECIPIENT = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/triggeringMessage/recipient/text()')) as MESSAGE_RECIPIENT"
val MESSAGE_CREATION_TS = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/creationTimestamp/text()')) as MESSAGE_CREATION_TS"
val MESSAGE_IS_TEST_FLAG = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/isTest/text()')) as MESSAGE_IS_TEST_FLAG"
val MESSAGE_ADDITIONAL_INFORMATION = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/additionalInformation/text()')) as MESSAGE_ADDITIONAL_INFORMATION"
val PMES_SEQUENCE_NUMBER = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/additionalInformation/pmesSequenceNumber/text()')) as PMES_SEQUENCE_NUMBER"
val TRIGGERING_MESSAGE_SENDER = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/triggeringMessage/sender/text()')) as TRIGGERING_MESSAGE_SENDER"
val TRIGGERING_MESSAGE_RECIPIENT = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/triggeringMessage/recipient/text()')) as TRIGGERING_MESSAGE_RECIPIENT"
val TRIGGERING_MESSAGE_CREATION_TS = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/triggeringMessage/creationTimestamp/text()')) as TRIGGERING_MESSAGE_CREATION_TS"
val TRIGGERING_MESSAGE_IS_TEST_FLAG = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/triggeringMessage/isTest/text()')) as TRIGGERING_MESSAGE_IS_TEST_FLAG"
val DSTID = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/dataSource/dstId/text()')) as DSTID"
val DATA_SOURCE_NAME = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/dataSource/name/text()')) as DATA_SOURCE_NAME"
val DATA_SOURCE_VERSION = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/dataSource/version/text()')) as DATA_SOURCE_VERSION"
val DETAIL_SOURCE_INFORMATION = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/dataSource/detailSourceInformation/text()')) as DETAIL_SOURCE_INFORMATION"
val MESSAGE_FORMAT_NAME = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/messageFormat/name/text()')) as MESSAGE_FORMAT_NAME"
val MESSAGE_FORMAT_VERSION = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/messageHeader/messageFormat/version/text()')) as MESSAGE_FORMAT_VERSION"
val DANGEROUS_GOODS_CODE = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/shipmentInformation/dangerousGoodsCode/text()')) as DANGEROUS_GOODS_CODE"
val DISTRIBUTION_PRODUCT_CODE = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/shipmentInformation/productInformation/distributionProductCode/text()')) as DISTRIBUTION_PRODUCT_CODE"
val HANDLING_CODE = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/shipmentInformation/productInformation/handlingCode/text()')) as HANDLING_CODE"
val ACTUAL_WEIGHT = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/packageInformation/measure/weight/@value')) as ACTUAL_WEIGHT"
val ACTUAL_WEIGHT_UNIT = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/packageInformation/measure/weight/@unit')) as ACTUAL_WEIGHT_UNIT"
val ACTUAL_WIDTH = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/packageInformation/measure/width/@value')) as ACTUAL_WIDTH"
val ACTUAL_WIDTH_UNIT = "concat_ws(',',xpath(value, '/packageNotification/packageBatchCard/events/event/packageInformation/measure/width/@unit')) as ACTUAL_WIDTH_UNIT"

