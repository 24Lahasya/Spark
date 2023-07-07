package com.scala.spark.exercises
import org.apache.spark.sql.SparkSession
object CreatRDD {
  def main(args: Array[String]): Unit = {
val spark= SparkSession.builder()
  .appName("FirstRDD")
  .master("local[1]")
  .getOrCreate()
    val rdd= spark.sparkContext.parallelize(Seq(("lahasya",1),("vamshi",2)))
    rdd.foreach(println)
    scala.io.StdIn.readLine()
  }
}


