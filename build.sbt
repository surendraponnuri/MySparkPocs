name := "sparkdocs"

version := "0.1"

scalaVersion := "2.11.12"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.4"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.4"

libraryDependencies += "com.datastax.spark" %% "spark-cassandra-connector" % "2.4.3"

// https://mvnrepository.com/artifact/com.datastax.spark/spark-cassandra-connector

