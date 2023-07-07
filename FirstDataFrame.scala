package com.scala.spark.exercises

import org.apache.spark.sql.SparkSession

object FirstDataFrame {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .appName("DF Demo")
      .master("local[1]")
      .getOrCreate()
    val data = Seq(("lahasya", "100000", 21, "Hyderabad"),
      ("vamshi", "85000", 24, "Warangal"),
      ("Deepak", "50000", 20, "Tirupathi"),
      ("Beula", "60000", 26, "Tuni"),
      ("Sujith", "400000", 22, "Vizag"))
    val col = Seq("Name", "Salary", "Age", "City")

    import spark.implicits._
    val rdd = spark.sparkContext.parallelize(data)

    val df = rdd.toDF(col: _*)
    df.show()



  }

  }



