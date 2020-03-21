package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// com.bigdata.spark.sparkcore.usdata
object usdata {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("usdata").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
//val data ="C:\\work\\datasets\\us-500.csv"
    val data =args(0)
    val output = args(1)
    val usrdd = sc.textFile(data) // rdd programming model
    val head = usrdd.first()
    val reg = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
  //  ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)" // means if any , avail within fields ignore such , .. that is this meaning.
    //seelct city, count(*) cnt from tab group by city order by cnt desc
    val pro = usrdd.filter(x=>x!=head).map(x=>x.split(reg)).map(x=>(x(6).replaceAll("\"",""),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
    pro.saveAsTextFile(output)
    pro.take(10).foreach(println)

    spark.stop()
  }
}