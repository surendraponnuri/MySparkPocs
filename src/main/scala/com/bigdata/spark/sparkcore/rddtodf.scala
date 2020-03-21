package com.bigdata.spark.sparkcore

import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object rddtodf {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.master("local[*]").appName("rddtodf").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext

    import spark.implicits._
    import spark.sql
    val data ="C:\\work\\datasets\\us-500.csv"
    //val data =args(0)
    //val output = args(1)
    val usrdd = sc.textFile(data) // rdd programming model
    val head = usrdd.first()
    val reg = ",(?=([^\"]*\"[^\"]*\")*[^\"]*$)"
    // clean data and make structure format
    val pro = usrdd.filter(x=>x!=head).map(x=>x.split(reg)).map(x=>(x(0).replaceAll("\"",""),x(1).replaceAll("\"","")
     , x(2).replaceAll("\"",""),x(3).replaceAll("\"",""),x(4).replaceAll("\"",""),x(5).replaceAll("\"",""),
      x(6).replaceAll("\"",""),x(7).replaceAll("\"",""),x(8).replaceAll("\"",""),x(9).replaceAll("\"","")
      , x(10).replaceAll("\"",""), x(11).replaceAll("\"","")))

    //rdd convert to datafrme
    val df = pro.toDF("first_name","last_name","company_name","address","city","county","state","zip","phone1","phone2","email","web")
//df.show()
    // to run sql query on top of dataframe use this createOrReplaceTempView
    df.createOrReplaceTempView("tab")
    //val res = spark.sql("select * from tab where state='CA' and email like '%gmail%'")
    val res = spark.sql("select state, count(*) cnt from tab group by state order by cnt desc")
    //res.write.format("csv").option("header","true").save(output)
    res.show()

   // pro.take(10).foreach(println)

    spark.stop()
  }
}