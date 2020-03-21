package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object onlyspark {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("onlyspark").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
def offers(state:String) = state match {
  case "NJ" | "NY" | "OH" => "10% off"
  case "LA" | "MI" | "AK" => "20% off"
  case "FL" | "CA" | "IL" => "30 % off"
  case _ => "no offer"
}
    val rep = (ph:String) => ph.replaceAll("-","")
    val offudf = udf(offers _)
    spark.udf.register("offerfunc",offudf)
    spark.udf.register("repminus",rep)
    val data = "C:\\work\\datasets\\us-500.csv"
    val df = spark.read.format("csv").option("header","true").option("inferSchema","true").load(data)
    df.createOrReplaceTempView("tab")
    val res = spark.sql("select first_name, offerfunc(state) todayoffer, state, repminus(phone1) phone1, repminus(phone2) phone2 from tab ")

df.show()
    spark.stop()
  }
}