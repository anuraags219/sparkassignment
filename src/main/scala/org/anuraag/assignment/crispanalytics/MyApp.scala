/*
 * Aadhar analysis for crispanalytics
 *
 * 
 */

package org.anuraag.assignment.crispanalytics

import org.apache.spark.sql.SparkSession

object MyApp {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    println("num lines: " + countLines(spark, args(0)))
  }

  def countLines(spark: SparkSession, path: String): Long = {
    spark.read.textFile(path).count()
  }
}

