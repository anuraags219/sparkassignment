/*
 * Visit data analysis for crispanalytics
 *
 * 
 */

package org.anuraag.assignment.crispanalytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.commons.codec.binary.Base64

case class Visit(email: String, mobno: String, visid: String)

object VisitAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    // Checkpoint 1

    val visitDataPath = args(0)
    val visitDataComoPath = args(1)

    val visitSchema = StructType(
      Seq(
      StructField("email", StringType),
      StructField("mobno", StringType),
      StructField("visid", StringType)
      )
    )
    
    val visitDetailsRaw = spark.read.format("csv")
      .schema(visitSchema)
      .option("header", "true")
      .load(visitDataPath)

    val visitDetails = visitDetailsRaw.as[Visit]

    val visitDetailsDecoded = visitDetails.map( visit =>
      Visit(
        decodeBase64(visit.email),
        decodeBase64(visit.mobno),
        visit.visid
      ) 
    )

    val visitDetailsDecodedDF = visitDetailsDecoded.toDF

	     
    val visitDetailsDecodedDF2 = visitDetailsDecodedDF.
    withColumnRenamed("email", "email2").
    withColumnRenamed("mobno", "mobno2").
    withColumnRenamed("visid", "visid2")

    val combinations = visitDetailsDecodedDF.join(
      visitDetailsDecodedDF2,
      (visitDetailsDecodedDF("email") === visitDetailsDecodedDF2("email2") ||
      visitDetailsDecodedDF("mobno") === visitDetailsDecodedDF2("mobno2")) &&
      (visitDetailsDecodedDF("visid") !== visitDetailsDecodedDF2("visid2")),
      "inner"
    )
    
    // coalesce to 1 to reduce the number of partitions as data size is small , not for production data
    combinations.select($"visid".as("visid1"), $"visid2").distinct
      .coalesce(1)
      .write.csv(visitDataComoPath)
    
  }
  
  def decodeBase64(encodedBase64: String): String = {
    new String(Base64.decodeBase64(encodedBase64), "UTF-8") 
  }
}

