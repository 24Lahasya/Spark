package com.scala.spark.exercises

import org.apache.spark.sql.SparkSession

object RDDMap {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder()
      .appName("Demo map method")
      .master("local[1]")
      .getOrCreate()
    val data = Seq("lahasya","vamshi","sagar","vinnu","nani","shour")

    import spark.sqlContext.implicits._
    val df=data.toDF("data")
    df.show()

    val mapDF=df.flatMap(fun=>{
      fun.getString(0)split(" ")
    })
    mapDF.show()
  }
}
