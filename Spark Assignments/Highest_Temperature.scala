package org.sparkproject.application

import org.apache.spark.SparkContext

object Highest_Temperature extends App {
  val sc = new SparkContext(master = "local[1]"/*No parallelism:*/, appName = "AppName" /*Application Name*/)

  //Read from text file
  val data = sc.textFile(path="C:/Users/siddh/OneDrive/Documents/Learning/Scala/spark/temp.txt")
  println("\n ")
  data.collect().foreach(println)  // Print all lines in the file

  val temperature = data.map(_.split(",")(2).toDouble).max()

  println(s"Highest Temperature :  $temperature")
}
