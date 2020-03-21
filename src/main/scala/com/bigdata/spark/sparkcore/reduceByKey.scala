package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object reduceByKey {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("reduceByKey").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data = "C:\\work\\datasets\\asl.txt"
    val usrdd = sc.textFile(data) // rdd programming model
    val head = usrdd.first()
    // 20% rdd, 70% dataframe, 10% dataset...2018
    // 80% using dataframe 20% using dataset...2019 feb
    //50% dataframe, 50% dataset
    //seelct city, count(*) cnt from tab group by city order by cnt desc
    val pro = usrdd.filter(x=>x!=head).map(x=>x.split(",")).map(x=>(x(2),1)).reduceByKey((a,b)=>a+b).sortBy(x=>x._2,false)
pro.collect.foreach(println)
    spark.stop()
  }
}
/*

    mas, (3)
    hyd,(2)
    del , (1)

  }
 */