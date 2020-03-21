package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object mapfilter {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("HelloWorld").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
val data="C:\\work\\datasets\\asl.txt"
    val aslrdd = sc.textFile(data)
    val fst = aslrdd.first()
    // select * from tab where city='hyd'
    // select count(*) , city from tab group by city;
    // jin two table

    val res = aslrdd.filter(x=>x!=fst).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2))).filter(x=>x._2<=32)
    //val res = aslrdd.filter(x=>x!=fst).map(x=>x.split(",")).map(x=>(x(0),x(1).toInt,x(2))).filter(x=>x._3=="hyd")
    res.collect.foreach(println)
    spark.stop()
  }
}