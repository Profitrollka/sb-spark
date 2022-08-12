import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object users_items {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder.appName("lab5").getOrCreate
    val modeType: Any = spark.sparkContext.getConf.getOption("spark.users_items.update").getOrElse(1)
    val inputDirPrefix: String =  spark.sparkContext.getConf.get("spark.users_items.input_dir")
    val outputDirPrefix: String = spark.sparkContext.getConf.get("spark.users_items.output_dir")

    val WindSpec = Window.partitionBy().orderBy(to_date(col("date"), "yyyyMMdd").desc)

    val view = spark.read.json(inputDirPrefix + "/view")
      .where(col("uid").isNotNull)
      .withColumn("max_date", first(to_date(col("date"), "yyyyMMdd")).over(WindSpec))
      .withColumn("new_item_id", concat(lit("view_"), regexp_replace(lower(col("item_id")), "-", "_")))
      .select("uid", "new_item_id", "max_date")

    val viewMatrix = view.groupBy(col("uid"), col("max_date")).pivot("new_item_id")
      .count().na.fill(0)

    val buy = spark.read.json(inputDirPrefix + "/buy")
      .where(col("uid").isNotNull)
      .withColumn("max_date", first(to_date(col("date"), "yyyyMMdd")).over(WindSpec))
      .withColumn("new_item_id", concat(lit("buy_"), regexp_replace(lower(col("item_id")), "-", "_")))
      .select("uid", "new_item_id", "max_date")

    val buyMatrix = buy.groupBy(col("uid"), col("max_date")).pivot("new_item_id")
      .count().na.fill(0)

    // объединенная просмотров и покупок матрица users * items
    val unionMatrix = viewMatrix.join(buyMatrix, Seq("uid", "max_date"), "full").na.fill(0)

    // определяем максимальную дату в объединенном df
    val maxDate = unionMatrix.select(col("max_date")).rdd.map(r => r(0)).collect()(1).toString.replaceAll("-", "")

    // удаляем колонку с максимальной датой
    val userItemMatrix = unionMatrix.drop(col("max_date"))

    if (modeType == 1) {
      // читаем старую таблицу и объединяем ее с новым df
      val combineMatrix = spark.read.parquet(outputDirPrefix).union(userItemMatrix)

      // пишем результата во временную директорию
      combineMatrix.write
        .mode("overwrite")
        .parquet(outputDirPrefix + "/" + maxDate)
    }
    else {
      userItemMatrix.write
        .mode("append")
        .parquet(outputDirPrefix + "/" + maxDate)
    }

  }

}
