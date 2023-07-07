package com.scalaspark.exercises

import org.apache.spark.sql.SparkSession

object Broadcast {
  def main(args: Array[String]): Unit = {
    val spark =SparkSession.builder()
      .appName("Broadcast")
      .master("local[1]")
      .getOrCreate()

    val inputRDD = spark.sparkContext.parallelize(Seq(("Lahasya","1000","USA","NY"),("Vamshi","2000","USA","TX"),("sagar","3000","IND","AP"),("sujith","20000","IND","TS"),("krishna","4000","AUS","QNS")))
   inputRdd.foreach(println)


    val countries = Map(("USA","United States of America"),("IND","India"),("AUS","Australia"))

    val states = Map(("NY","New York"),("TX","Texas"),("AP","Andhra Pradesh"),("TS","Telangana"),("QNS","Queens Land"))

    val broadcastStates = spark.sparkContext.broadcast(states)
    val broadcastCountries = spark.sparkContext.broadcast(countries)

    val finalRDD = inputRDD.map(f => {
      val country = f._3
      val state = f._4
      val fullCountry = broadcastCountries.value.get(country).get
      val fullState = broadcastStates.value.get(state).get
      (f._1,f._2,fullCountry,fullState)
    })

    println(finalRDD.collect().mkString("\n"))
  
    scala.io.StdIn.readLine();


  }
}