import org.apache.log4j._
import org.apache.spark.sql.{SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{MapType, StringType, StructType, IntegerType, TimestampType, DateType, LongType}

object filter {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.INFO)

    val spark = SparkSession.builder()
      .appName("lab04a")
      .getOrCreate()

    val offset: String = spark.sparkContext.getConf.get("spark.filter.offset")
    val topicName: String = spark.sparkContext.getConf.get("spark.filter.topic_name")
    val outputDirPrefix = spark.sparkContext.getConf.get("spark.filter.output_dir_prefix")

    val kafka_df = spark.read
      .format("kafka")
      .option("kafka.bootstrap.servers", "spark-master-1:6667")
      .option("subscribe", topicName)
      .option("startingOffsets", if (offset.contains("earliest")) offset
      else {"{\"" + topicName + "\":{\"0\":" + offset + "}}"})
      .load

    val kafkaValue = kafka_df.select(col("value").cast("string")).toDF

    // определяем схему данных для извлечения JSON
    val schema = new StructType()
      .add("event_type", StringType, true)
      .add("category", StringType, true)
      .add("item_id", StringType, true)
      .add("item_price", IntegerType, true)
      .add("uid", StringType, true)
      .add("timestamp", StringType, true)

    spark.conf.set("spark.sql.session.timeZone", "UTC")
    val events = kafkaValue.withColumn("value",from_json(col("value"), schema))
      .select(col("value.*"))
      .withColumn("date", regexp_replace(to_date(
        from_unixtime(col("timestamp") / 1000)).cast("string"), "-", ""))
      .withColumn("p_date", regexp_replace(to_date(
        from_unixtime(col("timestamp") / 1000)).cast("string"), "-", ""))

    // отбираем просмотры
    events.where(col("event_type") === "view").orderBy("date")
      .write.partitionBy("p_date").json(outputDirPrefix + "/view")

    // отбираем покупки
    events.where(col("event_type") === "buy").orderBy("date")
      .write.partitionBy("p_date").json(outputDirPrefix + "/buy")
  }
}
