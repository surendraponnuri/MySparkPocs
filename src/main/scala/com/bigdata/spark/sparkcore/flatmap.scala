package com.bigdata.spark.sparkcore

import org.apache.spark.sql._

object flatmap {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("flatmap").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
    //spark 4 contexts available
    //context means a nutshell to create diff api.
    //rdd api ... spakContext
   val data = "C:\\work\\datasets\\asl.txt"
    val usrdd = sc.textFile(data) // rdd programming model
    val head = usrdd.first()
    val pro = usrdd.filter(x=>x!=head).map(x=>x.split(",")).filter(x=>x(1).toInt>32) //Array("venu",32,"mas")... map(x=>x(2)=='hyd')
      pro.collect.foreach(println)


/*//val data = "C:\\work\\datasets\\chathistory.txt"
    val rdd = sc.textFile(data)
      val process = rdd.flatMap(x=>x.split(" ")).filter(x=>x.contains("@") && !(x.contains("83")))
    //spark 3 apis ... rdd api, dataframe api, dataset api
    process.collect.foreach(println)*/

    spark.stop()
  }
}