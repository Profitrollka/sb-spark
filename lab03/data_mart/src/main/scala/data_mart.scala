import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import java.net.URLDecoder
import scala.util.{Try, Success, Failure}
import java.sql.DriverManager
import java.sql.Connection
import java.sql.ResultSet

object data_mart {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val password = "PpMTdWqs"
    val urlPostgre = "jdbc:postgresql://10.0.0.31:5432/"
    val loginPostgre = "alyona_kapustyan"
    val loginElastic = "alyona.kapustyan"
    val driverPostgre = "org.postgresql.Driver"
    val portElastic = "9200"
    val hostElastic = "10.0.0.31"

    // информация о тематических категориях веб-сайтов, приобретенную у вендора
    val sourcePostgreSchema = "labdata"
    val sourcePostgreDomainCats = "domain_cats"

    // информация о посещениях сторонних веб-сайтов пользователями, приобретенная у вендора
    val sourceHdfsLogs = "hdfs:///labs/laba03/weblogs.json"

    // информация о клиентах
    val sourceCassandraKeyspace = "labdata"
    val sourceCassandraClients = "clients"

    // логи посещения интернет-магазина
    val sourceElastcVisits = "visits"

    // финальная талица
    val finalTableSchema = "alyona_kapustyan"
    val finalTableName = "clients"


    val spark = SparkSession.builder()
      .master("yarn")
      .appName("lab03")
      .getOrCreate()

    // загружаем информацию о тематических категориях веб-сайтов, приобретенную у вендора
    val webCategory = spark.read
      .format("jdbc")
      .options(Map("url" -> f"$urlPostgre$sourcePostgreSchema",
        "dbtable" -> sourcePostgreDomainCats,
        "user" -> loginPostgre,
        "password" -> password,
        "driver" -> driverPostgre))
      .load()

    // загружаем логи посещения сторонних веб-сайтов пользователями, приобретенные у вендора.
    val foreignLogs = spark.read.json(sourceHdfsLogs)
      .select(col("uid"), explode(col("visits")))
      .select("uid", "col.*")
      .where(col("uid").isNotNull)

    // определяем функцию для декодирования URL
    val urlDecoder = udf { (url: String) =>
      Try(URLDecoder.decode(url, "UTF-8")) match {
        case Success(url: String) => url
        case Failure(exc) => ""
      }
    }

    // декодируем URL в полученном df, извлекаем домен
    val pattern: String = "([^:\\/\\n?]+)/?()"
    val groupId: Int = 1
    val logsDecocedUrl = foreignLogs.select(col("uid"), urlDecoder(col("url")).as("decoded_url"))
      .where(!(col("decoded_url") === ""))
      .withColumn("tmp_domain", regexp_replace(col("decoded_url"), "^https?://(www.)?", ""))
      .withColumn("domain", regexp_extract(col("tmp_domain"), pattern, groupId))
      .select("uid", "domain")

    // определяем категории посещенных веб сайтов по доменам и считаем количество посещений по категориям
    val webSitesCategory = logsDecocedUrl.join(webCategory, Seq("domain"), "inner")
      .select("uid", "category")
      .withColumn("web_cat", concat(lit("web_"), regexp_replace(
        regexp_replace(lower(col("category")), " ", "_")
        , "-", "_")))
      .groupBy("uid").pivot("web_cat").count.na.fill(value=0)

    // определяем параметры подключения к Cassandra
    spark.conf.set("spark.cassandra.connection.host", "10.0.0.31")
    spark.conf.set("spark.cassandra.connection.port", "9042")
    spark.conf.set("spark.cassandra.output.consistency.level", "ANY")
    spark.conf.set("spark.cassandra.input.consistency.level", "ONE")

    // загружаем данные о клиентах
    val clients = spark.read
      .format("org.apache.spark.sql.cassandra")
      .options(Map("table" -> sourceCassandraClients,"keyspace" -> sourceCassandraKeyspace))
      .load()

    // определяем возрастные категории клиентов
    val clientsWithAgeCategory = clients.withColumn("age_cat", when(((col("age") >= 18)
      &&(col("age") <= 24)), "18-24")
      .when(((col("age") >= 25)&&(col("age") <= 34)), "25-34")
      .when(((col("age") >= 35)&&(col("age") <= 44)), "35-44")
      .when(((col("age") >= 45)&&(col("age") <= 54)), "45-54")
      .when(col("age") >= 55, ">=55"))
      .select("uid", "gender", "age_cat")

    // добавляем к пользователям инфо о посещении различных категорий веб сайтов
    val webSitesCategoryWithClients = clientsWithAgeCategory.join(webSitesCategory,
      Seq("uid"), "left")

    // загружаем логи посещения интернет магазина
    val visitsElastic = spark.read.format("org.elasticsearch.spark.sql")
      .options(Map("es.port" -> portElastic,
        "es.nodes" -> hostElastic,
        "es.net.http.auth.user" -> loginElastic,
        "es.net.http.auth.pass" -> password))
      .load(sourceElastcVisits)

    // считаем количество просмотров различных категорий интернет магазина
    val shopLogs = visitsElastic.where(col("uid").isNotNull)
      .withColumn("shop_cat", concat(lit("shop_"),
        regexp_replace(
          regexp_replace(lower(col("category")), " ", "_")
          , "-", "_")))
      .select("uid", "shop_cat")
      .groupBy("uid")
      .pivot("shop_cat")
      .count
      .na.fill(value=0)

    // добавляем к пользователям инфо о просмотре различных категорий интернет магазина
    val shopLogsWithCategory = clientsWithAgeCategory.join(shopLogs, Seq("uid"), "left")

    // объединяем данные о просмотренных/приобретенных категориях товаров собственного интернет магазина и сторонних вюэб сайтов
    val finalTable = shopLogsWithCategory.join(webSitesCategoryWithClients, Seq("uid", "gender", "age_cat"), "outer")
      .na.fill(value=0)

    // сохраняем финальную таблицу
    finalTable.write
      .format("jdbc")
      .option("url", f"$urlPostgre$finalTableSchema")
      .option("dbtable", finalTableName)
      .option("user", loginPostgre)
      .option("password", password)
      .option("driver", driverPostgre)
      .mode("overwrite")
      .save()

    // предоставляем права чекеру на таблицу
    val query = s"GRANT SELECT on clients to PUBLIC"
    val connector = DriverManager.getConnection(f"$urlPostgre$sourcePostgreSchema?user=$loginPostgre&password=$password")
    val resultSet = connector.createStatement.execute(query)
  }

  }


