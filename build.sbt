name := "AvroList"

version := "0.1"

scalaVersion := "2.11.12"

resolvers += "Cloudera Repository" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "com.typesafe" % "config" % "1.3.2"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"

libraryDependencies += "org.apache.hbase" % "hbase-spark" % "1.2.0-cdh5.14.0"

libraryDependencies += "org.apache.hbase" % "hbase" % "1.2.0"

// https://mvnrepository.com/artifact/org.apache.spark/spark-avro
//libraryDependencies += "com.databricks" %% "spark-avro" % "4.0.0"


