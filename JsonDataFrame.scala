package com.scala.spark.exercises

import org.apache.spark.sql.SparkSession

object JsonDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("Json File")
      .master("local[1]")
      .getOrCreate()

    val df = spark.read.option("header", true)
      .json("ebook.json")
    df.show()

  }
}
