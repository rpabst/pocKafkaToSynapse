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

handleStreamToMem(inputFromKafkaStreamCastWithPk)


def handleStreamToMem(df: DataFrame) = {
   df
  .writeStream
  .queryName("dbtable")    // this query name will be the table name
  .outputMode("append")
  .format("memory")
  .start()
  
}


def addHashColumn(df_tmp: DataFrame, newColName: String): DataFrame = {
  val columns = df_tmp.columns.map(col)
  
  df_tmp.withColumn(newColName, xxhash64(columns:_*))
} 


