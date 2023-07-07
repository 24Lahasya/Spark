package com.scala.spark.exercises

import org.apache.spark.sql.SparkSession

object ReadTextFileRDD {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Reading The Text File")
      .master("local[1]")
      .getOrCreate()
    val rdd = spark.sparkContext.textFile("textfile.txt")
    rdd.foreach(println)
  }
}
