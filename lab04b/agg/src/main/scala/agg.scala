import org.apache.log4j._
import org.apache.spark.sql.{SparkSession, DataFrame}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructType, IntegerType, TimestampType, DoubleType}


object agg {

  def main(args: Array[String]) = {

    Logger.getLogger("org").setLevel(Level.INFO)

    val spark = SparkSession.builder()
      .appName("lab04b")
      .getOrCreate()

    val kafkaParams = Map(
      "kafka.bootstrap.servers" -> "spark-master-1.newprolab.com:6667",
      "subscribe" -> "alyona_kapustyan",
      "startingOffsets" -> "earliest"
    )
    val checkpointLocation = "/user/alyona.kapustyan/lab04b/checkpoint"
    val trigger = "30 seconds"
    val mode = "update"
    val output = "alyona_kapustyan_lab04b_out"

    // читаем данные из топика
    val kafkaMessages = spark.readStream.format("kafka").options(kafkaParams).load()

    // извлекаем value из сообщений kafka
    val kafkaMessagesVal = kafkaMessages.select(col("value").cast("string"))

    // определяем схему данных для извлечения JSON
    val schema = new StructType()
      .add("event_type", StringType, true)
      .add("category", StringType, true)
      .add("item_id", StringType, true)
      .add("item_price", IntegerType, true)
      .add("uid", StringType, true)
      .add("timestamp", TimestampType, true)

    val eventsDf = kafkaMessagesVal.withColumn("value",from_json(col("value"), schema))
      .select(col("value.*"))

    // считаем агрегаты
    val eventsAggDf = eventsDf.groupBy(window((col("timestamp").cast("bigint") / 1000).cast(TimestampType), "60 minutes"))
      .agg(sum(when(col("event_type") === "buy", col("item_price"))).alias("revenue"),
        sum(when(col("uid").isNotNull, lit(1)).otherwise(lit(0))).alias("visitors"),
        sum(when(col("event_type") === "buy", lit(1)).otherwise(lit(0))).alias("purchases"))
      .withColumn("aov", col("revenue")/col("purchases"))
      .withColumn("start_ts", unix_timestamp(col("window.start")))
      .withColumn("end_ts", unix_timestamp(col("window.end")))
      .select("start_ts", "end_ts", "revenue", "visitors", "purchases", "aov")

    def createKafkaSinc(trigger: String, mode: String, output: String, chkName: String,  df: DataFrame) = {
      df.selectExpr("to_json(struct(*)) AS value")
        .writeStream
        .format("kafka")
        .trigger(Trigger.ProcessingTime(trigger))
        .outputMode(mode)
        .option("kafka.bootstrap.servers", "spark-master-1:6667")
        .option("topic", output)
        .option("checkpointLocation", chkName)
        .option("failOnDataLoss",false)
    }

    // пишем данные в кафку
    createKafkaSinc(trigger, mode, output, checkpointLocation, eventsAggDf).start.awaitTermination

  }
}
