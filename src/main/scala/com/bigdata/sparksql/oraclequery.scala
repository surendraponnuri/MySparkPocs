package com.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object oraclequery {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("oraclequery").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val ourl = "jdbc:oracle:thin:@//paritoshoracle.czhyzqz8vleo.ap-south-1.rds.amazonaws.com:1521/orcl"
    val oprop = new java.util.Properties()
    oprop.setProperty("user", "ousername")
    oprop.setProperty("password", "opassword")
    oprop.setProperty("driver", "oracle.jdbc.OracleDriver")
    val query = "(select * from EMP where sal<12000) t"
    val df = spark.read.jdbc(ourl, query, oprop)
    df.show()
    spark.stop()
  }
}