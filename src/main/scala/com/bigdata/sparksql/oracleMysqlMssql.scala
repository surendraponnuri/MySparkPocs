package com.bigdata.sparksql

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
// com.bigdata.sparksql.oracleMysqlMssql
object oracleMysqlMssql {
  def main(args: Array[String]) {
    val spark = SparkSession.builder.appName("csvdata").getOrCreate()
    // val ssc = new StreamingContext(spark.sparkContext, Seconds(10))
    val sc = spark.sparkContext
val output = args(0)
    val s3path= s"s3://surendrabucket2020/output/$output"
    import spark.implicits._
    import spark.sql
    //val ourl = "jdbc:oracle:thin:@//paritoshoracle.czhyzqz8vleo.ap-south-1.rds.amazonaws.com:1521/orcl"
//    val df1 = spark.read.format("jdbc").option("url",ourl).option("dbtable","EMP").option("driver","").option("user","").option("password","").load()
val df1 = spark.read.format("jdbc").option("url","jdbc:oracle:thin:@//paritoshoracle.czhyzqz8vleo.ap-south-1.rds.amazonaws.com:1521/orcl").option("dbtable","EMP").option("driver","oracle.jdbc.OracleDriver").option("user","ousername").option("password","opassword").load()
df1.show()

    // another way to get data from other database
    val msurl = "jdbc:sqlserver://venudb.cnwu9xc3aep7.ap-south-1.rds.amazonaws.com:1433;databaseName=venutasks;"
    val prop = new java.util.Properties()
    prop.setProperty("user","msusername")
    prop.setProperty("password","mspassword")
    prop.setProperty("driver","com.microsoft.sqlserver.jdbc.SQLServerDriver")

    val df2 = spark.read.jdbc(msurl,"dept",prop)
    df2.show()

    // data processing
    df1.createOrReplaceTempView("emp")
    df2.createOrReplaceTempView("dept")
    val query = "select d.loc, d.dname, e.ename, e.sal, e.job from emp e join dept d on e.deptno=d.deptno where cast(sal as int) >12000"
    val res = spark.sql(query)
    res.show()
    val murl= "jdbc:mysql://dbmysql.ckyod3xssluv.ap-south-1.rds.amazonaws.com:3306/dbmysql"
    val mprop=new java.util.Properties()
    mprop.setProperty("user","musername")
    mprop.setProperty("password","mpassword")
    mprop.setProperty("driver","com.mysql.jdbc.Driver")
    res.write.jdbc(murl,output,mprop)
    res.write.mode(SaveMode.Overwrite).format("csv").option("header","true").save(s3path)
    res.write.mode(SaveMode.Overwrite).format("hive").saveAsTable(output)

    //val query = "select * from tab where state='OH'"
    spark.stop()
  }
}