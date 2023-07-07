package com.scala.spark.exercises

import org.apache.spark.sql.SparkSession

object CSVDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF Demo")
      .master("local[1]")
      .getOrCreate()

    val df = spark.read.option("header",true)
      .csv("src/main/scala/com/scala/spark/exercises/emp.csv")
    df.show()
    df.select("name","age").show()

  }
}


