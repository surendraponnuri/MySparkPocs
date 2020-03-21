package com.bigdata

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object SparkCassandra {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("SparkCassandra").enableHiveSupport().getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
   val df=spark.read.format(source = "org.apache.spark.sql.cassandra").option("keyspace","testdb").option("table","asl").load()
    val df1=spark.read.format(source = "org.apache.spark.sql.cassandra").option("keyspace","testdb").option("table","nep").load()
    //df.show()
    //df1.show()
    val tab=args(0)
    df.createOrReplaceTempView(viewName = "asl")
    df1.createOrReplaceTempView(viewName = "nep")
    val res=spark.sql(sqlText = "select a.name,a.city,n.email,n.phone from asl a join nep n on a.name=n.name")
    res.show()
    //To store data in hive use below
    //res.write.mode(SaveMode.Overwrite).format(source="hive").saveAsTable(tableName = "nepasljoin")
    //to store data by mentioning args0 instead of mentioning tablename use below
    //res.write.mode(SaveMode.Overwrite).format(source="hive").saveAsTable(args(0))
    //To store args(0)value in variable and pass that variable below
    res.write.mode(SaveMode.Overwrite).format(source="hive").saveAsTable(tab)
    //To store result in cassandra use below
   // res.write.mode(SaveMode.Overwrite).format(source ="org.apache.spark.sql.cassandra" ).option("keyspace","testdb").option("table","asl").load()
    //before exporting to cassandra creating table in advance
    //create table aslnepjoin(name varchar,city varchar,email varchar,phone int,primary key(name);
    spark.sql(
      s"""
         |CREATE TEMPORARY TABLE test
         |USING org.apache.spark.sql.cassandra
         |OPTIONS (
         | table test,
         | keyspace "testdb",
         | confirm.truncate "true")
      """.stripMargin.replaceAll("\n", " "))

    res.write.mode(SaveMode.Append).format(source ="org.apache.spark.sql.cassandra" ).option("keyspace","testdb").option("table",tab).save()
    spark.stop()
  }
}