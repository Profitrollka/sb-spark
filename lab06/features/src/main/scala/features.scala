import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object features {

  val spark = SparkSession.builder.appName("lab06").getOrCreate
  val outputDir = "/user/alyona.kapustyan/features"
  val inputLogs = "/labs/laba03/"
  val inputUserItemsMatrix = "./users-items/20200429"

  val webLogs = spark.read.json(inputLogs)
    .select(col("uid"), explode(col("visits")))
    .select(col("uid"), col("col.*"))
    .where(col("uid").isNotNull)

  // отбираем топ 1000 доменов по посещаемости без привязки к пользователю
  val top1000Domains = webLogs.withColumn("domain_top",
    regexp_replace(lower(
      callUDF("parse_url", col("url"), lit("HOST"))),"www.",""))
    .where(!(col("domain_top") === ""))
    .groupBy(col("domain_top")).count
    .orderBy(col("count").desc)
    .limit(1000)
    .select("domain_top")
    .orderBy(col("domain_top"))

  spark.conf.set("spark.sql.session.timeZone", "UTC")
  val webLogsDomains = webLogs.withColumn("domain",
    regexp_replace(lower(
      callUDF("parse_url", col("url"), lit("HOST"))),"www.",""))
    .withColumn("datetime", to_timestamp(from_unixtime
    (col("timestamp") / 1000)))
    .select("uid", "domain", "datetime")

  val webLogsDomainsWithTop = webLogsDomains.join(top1000Domains, webLogsDomains("domain")
    === top1000Domains("domain_top"),"outer")
    .select("uid", "domain_top")
    .groupBy("uid").pivot("domain_top").count
    .na.fill(0)

  // формируем вектор с числами посещений каждого сайта из топ-1000 по посещениям или 0, если не посещал
  val colNames = webLogsDomainsWithTop.drop("uid", "null").schema.fieldNames
  val domainExpr = "array(" + colNames.map (x => "`"+ x +"`").mkString(",") + ") as domain_features"
  val domainFeaturesDf = webLogsDomainsWithTop.drop("null").selectExpr("uid", domainExpr)

  // Формируем доп признаки: число посещений по дням недели, число посещений по часам в сутках.
  // долю посещений в рабочие часы (число посещений в рабочие часы/общее число посещений данного клиента)
  // долю посещений в вечерние часы "web_fraction_evening_hours", где рабочие часы - [9, 18), а вечерние - [18, 24).
  val webLogsDateTimeFeatures = webLogsDomains.withColumn("web_day_mon", when(dayofweek(col("datetime")) === 2, 1.0)
    .otherwise(0.0))
    .withColumn("web_day_tue", when(dayofweek(col("datetime")) === 3, 1.0)
      .otherwise(0.0))
    .withColumn("web_day_wed", when(dayofweek(col("datetime")) === 4, 1.0)
      .otherwise(0.0))
    .withColumn("web_day_thu", when(dayofweek(col("datetime")) === 5, 1.0)
      .otherwise(0.0))
    .withColumn("web_day_fri", when(dayofweek(col("datetime")) === 6, 1.0)
      .otherwise(0.0))
    .withColumn("web_day_sat", when(dayofweek(col("datetime")) === 7, 1.0)
      .otherwise(0.0))
    .withColumn("web_day_sun", when(dayofweek(col("datetime")) === 1, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_0", when(hour(col("datetime")) === 0, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_1", when(hour(col("datetime")) === 1, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_2", when(hour(col("datetime")) === 2, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_3", when(hour(col("datetime")) === 3, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_4", when(hour(col("datetime")) === 4, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_5", when(hour(col("datetime")) === 5, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_6", when(hour(col("datetime")) === 6, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_7", when(hour(col("datetime")) === 7, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_8", when(hour(col("datetime")) === 8, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_9", when(hour(col("datetime")) === 9, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_10", when(hour(col("datetime")) === 10, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_11", when(hour(col("datetime")) === 11, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_12", when(hour(col("datetime")) === 12, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_13", when(hour(col("datetime")) === 13, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_14", when(hour(col("datetime")) === 14, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_15", when(hour(col("datetime")) === 15, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_16", when(hour(col("datetime")) === 16, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_17", when(hour(col("datetime")) === 17, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_18", when(hour(col("datetime")) === 18, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_19", when(hour(col("datetime")) === 19, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_20", when(hour(col("datetime")) === 20, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_21", when(hour(col("datetime")) === 21, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_22", when(hour(col("datetime")) === 22, 1.0)
      .otherwise(0.0))
    .withColumn("web_hour_23", when(hour(col("datetime")) === 23, 1.0)
      .otherwise(0.0))
    .groupBy("uid")
    .agg(sum("web_day_mon").alias("web_day_mon"),
      sum("web_day_tue").alias("web_day_tue"),
      sum("web_day_wed").alias("web_day_wed"),
      sum("web_day_thu").alias("web_day_thu"),
      sum("web_day_fri").alias("web_day_fri"),
      sum("web_day_sat").alias("web_day_sat"),
      sum("web_day_sun").alias("web_day_sun"),
      sum("web_hour_0").alias("web_hour_0"),
      sum("web_hour_1").alias("web_hour_1"),
      sum("web_hour_2").alias("web_hour_2"),
      sum("web_hour_3").alias("web_hour_3"),
      sum("web_hour_4").alias("web_hour_4"),
      sum("web_hour_5").alias("web_hour_5"),
      sum("web_hour_6").alias("web_hour_6"),
      sum("web_hour_7").alias("web_hour_7"),
      sum("web_hour_8").alias("web_hour_8"),
      sum("web_hour_9").alias("web_hour_9"),
      sum("web_hour_10").alias("web_hour_10"),
      sum("web_hour_11").alias("web_hour_11"),
      sum("web_hour_12").alias("web_hour_12"),
      sum("web_hour_13").alias("web_hour_13"),
      sum("web_hour_14").alias("web_hour_14"),
      sum("web_hour_15").alias("web_hour_15"),
      sum("web_hour_16").alias("web_hour_16"),
      sum("web_hour_17").alias("web_hour_17"),
      sum("web_hour_18").alias("web_hour_18"),
      sum("web_hour_19").alias("web_hour_19"),
      sum("web_hour_20").alias("web_hour_20"),
      sum("web_hour_21").alias("web_hour_21"),
      sum("web_hour_22").alias("web_hour_22"),
      sum("web_hour_23").alias("web_hour_23"))
    .withColumn("web_fraction_work_hours",
      (col("web_hour_9") + col("web_hour_10") +
        col("web_hour_11") + col("web_hour_12") +
        col("web_hour_13") + col("web_hour_14") +
        col("web_hour_15") + col("web_hour_16") +
        col("web_hour_17") + col("web_hour_18"))
        / (col("web_hour_0") + col("web_hour_1") +
        col("web_hour_2") + col("web_hour_3") +
        col("web_hour_4") + col("web_hour_5") +
        col("web_hour_6") + col("web_hour_7") +
        col("web_hour_8") + col("web_hour_9") +
        col("web_hour_10") + col("web_hour_11") +
        col("web_hour_12") + col("web_hour_13") +
        col("web_hour_14") + col("web_hour_15") +
        col("web_hour_16") + col("web_hour_17") +
        col("web_hour_18") + col("web_hour_19") +
        col("web_hour_20") + col("web_hour_21") +
        col("web_hour_22") + col("web_hour_23")))
    .withColumn("web_fraction_evening_hours",
      (col("web_hour_19") + col("web_hour_20") +
        col("web_hour_21") + col("web_hour_22") +
        col("web_hour_23"))
        /(col("web_hour_0") + col("web_hour_1") +
        col("web_hour_2") + col("web_hour_3") +
        col("web_hour_4") + col("web_hour_5") +
        col("web_hour_6") + col("web_hour_7") +
        col("web_hour_8") + col("web_hour_9") +
        col("web_hour_10") + col("web_hour_11") +
        col("web_hour_12") + col("web_hour_13") +
        col("web_hour_14") + col("web_hour_15") +
        col("web_hour_16") + col("web_hour_17") +
        col("web_hour_18") + col("web_hour_19") +
        col("web_hour_20") + col("web_hour_21") +
        col("web_hour_22") + col("web_hour_23")))

  // добавляем к матрице ветктор с числами посещений каждого сайта из топ-1000
  val webLogMatrix = webLogsDateTimeFeatures.join(domainFeaturesDf, Seq("uid"), "outer")

  val userItemMatrix = spark.read.parquet(inputUserItemsMatrix)

  // объединяем матрицу users * items (данные о просмотрах и покупках пользователей) с данными о посещенияю веб сайтов
  val finalMatrix = webLogMatrix.join(userItemMatrix, Seq("uid"), "outer")

  finalMatrix.write.mode("overwrite").parquet(outputDir)

}
