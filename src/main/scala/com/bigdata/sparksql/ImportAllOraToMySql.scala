package com.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object ImportAllOraToMySql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("ImportAllOraToMySql").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val ourl = "jdbc:oracle:thin:@//paritoshoracle.czhyzqz8vleo.ap-south-1.rds.amazonaws.com:1521/orcl"
    val oprop = new java.util.Properties()
    oprop.setProperty("user", "ousername")
    oprop.setProperty("password", "opassword")
    oprop.setProperty("driver", "oracle.jdbc.OracleDriver")

    val murl= "jdbc:mysql://dbmysql.ckyod3xssluv.ap-south-1.rds.amazonaws.com:3306/dbmysql"
    val mprop=new java.util.Properties()
    mprop.setProperty("user","musername")
    mprop.setProperty("password","mpassword")
    mprop.setProperty("driver","com.mysql.jdbc.Driver")
//val tabs = Array("EMP","DEPT","NEWEMP")
  //  val query = "(SELECT  table_name FROM  all_tables where TABLESPACE_NAME='USERS' AND TABLE_NAME <> 'DATA_2008') t"
    val query = "(select table_name from information_schema.tables where TABLE_SCHEMA='dbmysql') t"
    val tabs = spark.read.jdbc(ourl, query, oprop).select("TABLE_NAME").rdd.map(x=>x(0)).collect.toList

    tabs.foreach { x =>
     val df = spark.read.jdbc(ourl, s"$x", oprop)
     // df.write.mode(SaveMode.Overwrite).jdbc(murl,s"$x",mprop)
      df.show()
    }



    spark.stop()
  }
}