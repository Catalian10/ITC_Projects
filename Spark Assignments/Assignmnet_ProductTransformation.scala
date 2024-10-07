package org.sparkproject.application

import org.apache.spark.SparkConf
import org.apache.spark.sql.{SparkSession, functions}
import org.apache.spark.sql.functions.{col, lit, lower, trim}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window

object Assignmnet_ProductTransformation extends App{
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "dataframeDemo")
  sparkConf.set("spark.master", "local[1]")
  val ss = SparkSession.builder().config(sparkConf).getOrCreate()

  val ddlSchema =
    """
      product_number STRING,
      product_name STRING,
      product_category STRING,
      product_manufacturer STRING,
      length DOUBLE,
      width DOUBLE,
      height DOUBLE,
      col1 DOUBLE,
      col2 DOUBLE
  """

  // Reading the CSV file with the defined schema
  val productdf = ss.read.option("header", true).schema(ddlSchema).csv("C:/Users/siddh/Desktop/Training/products/output/try1.csv")
  println("Displaying the first 5 rows of the product DataFrame after reading from CSV")
  productdf.show(numRows = 5)

  // 1. Check for outliers in length and width columns using percentile
  val quantiles = productdf.stat.approxQuantile(Array("length", "width"), Array(0.25, 0.75), 0.05)

  // Ensure quantiles were properly calculated
  if (quantiles.length == 2 && quantiles(0).length == 2 && quantiles(1).length == 2) {
    // Extracting lower and upper quartiles for length and width
    val (lengthLower, lengthUpper) = (quantiles(0)(0), quantiles(0)(1))
    val (widthLower, widthUpper) = (quantiles(1)(0), quantiles(1)(1))
    val iqrLength = lengthUpper - lengthLower
    val iqrWidth = widthUpper - widthLower

    // Filtering out outliers using IQR (Interquartile Range)
    val filteredDf = productdf.filter(col("length").between(lengthLower - 1.5 * iqrLength, lengthUpper + 1.5 * iqrLength) &&
      col("width").between(widthLower - 1.5 * iqrWidth, widthUpper + 1.5 * iqrWidth))

    // 2. Separate product_number into storeid and productid using _ as separator
    // Split 'product_number' column into 'storeid' and 'productid'
    val splitCol = functions.split(col("product_number"), "_")
    val cleanedDf = filteredDf.withColumn("storeid", splitCol.getItem(0)).withColumn("productid", splitCol.getItem(1))

    // 3. Separate year from product_name into a new year column
    // Extract year from 'product_name' using regex pattern for four-digit year
    val yearPattern = "\\b(\\d{4})\\b"
    val cleanedDfWithYear = cleanedDf.withColumn("year", regexp_extract(col("product_name"), yearPattern, 0))

    println("Displaying the first 5 rows of the cleaned DataFrame with storeid, productid, and year columns added")
    cleanedDfWithYear.show(5)

    // Data Transformation Section
    // 1. Add a new column called product_size that categorizes products based on length
    // Categorize products based on their length into Small, Medium, Large, or Extra Large
    val transformedDf = cleanedDfWithYear.withColumn("product_size",
      when(col("length") < 1000, "Small")
        .when(col("length").between(1000, 2000), "Medium")
        .when(col("length").between(2000, 3000), "Large")
        .otherwise("Extra Large")
    )

    // 2. Create pivot based on product_category and product_size and count the products
    // Pivot the data to show counts of each product_size within each product_category
    val pivotDf = transformedDf.groupBy("product_category").pivot("product_size").count()
    println("Displaying the pivot table with product counts by category and size")
    pivotDf.show()

    // 3. Use a window function to rank products within each category based on their length and display the second longest product
    // Define a window specification to rank products within each product_category based on length in descending order
    val windowSpec = Window.partitionBy("product_category").orderBy(col("length").desc)
    val rankedDf = transformedDf.withColumn("rank", rank().over(windowSpec))

    // Filter to get the second longest product within each product_category
    val secondLongestProductDf = rankedDf.filter(col("rank") === 2)
    println("Displaying the second longest product for each product category")
    secondLongestProductDf.show(5)
  } else {
    // Print an error message if quantiles could not be calculated properly
    println("Error: Unable to calculate quantiles for outlier detection. Please check the input data.")
  }
}
