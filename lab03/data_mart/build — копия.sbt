version := "0.1"

scalaVersion := "2.11.12"

name := "data_mart"

  libraryDependencies ++= Seq(
    "org.apache.spark" %% "spark-core" % "2.4.8",
    "org.apache.spark" %% "spark-sql" % "2.4.8",
    "org.apache.spark" %% "spark-mllib" % "2.4.8",
    "org.apache.spark" %% "spark-streaming" % "2.4.8",
    "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3",
    "org.elasticsearch" %% "elasticsearch-spark-20" % "7.6.2",
    "org.postgresql" % "postgresql" % "42.2.12"
  )