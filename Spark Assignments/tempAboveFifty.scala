package org.sparkproject.application

import org.apache.spark.SparkContext

object tempAboveFifty extends App {

  // Step 1: Initialize SparkContext
  val sc = new SparkContext(master="local[1]", appName = "tempAbove50")

  // Step 2: Read from text file
  val data = sc.textFile("C:/Users/siddh/OneDrive/Documents/Learning/Scala/spark/temp.txt")
  println("\n ")
  data.collect().foreach(println)  // Print all lines in the file

  // Step 3: Extract sensor and temperature, filter temperatures greater than 50
  val sensorAboveFifty = data
    .map { line =>
      val parts = line.split(",")
      val sensor = parts(0)       // Extract sensor (first column)
      val temp = parts(2).toDouble // Extract temperature (third column)
      (sensor, temp)
    }
    .filter { case (sensor, temp) => temp > 50.0 } // Filter temperatures greater than 50

  // Step 4: Count how many times each sensor has recorded temperature above 50
  val countAboveFifty = sensorAboveFifty
    .map { case (sensor, temp) => (sensor, 1) }    // Map to (sensor, 1)
    .reduceByKey(_ + _)                            // Sum the counts for each sensor

  // Step 5: Print the result (sensor, count)
  countAboveFifty.collect().foreach {
    case (sensor, count) => println(s"$count, $sensor")
  }

  // Stop the SparkContext
  sc.stop()
}
