/*
 * Aadhar analysis for crispanalytics
 *
 * 
 */

package org.anuraag.assignment.crispanalytics

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.apache.spark.ml.feature.Bucketizer

object AadharAnalysis {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().getOrCreate()
    import spark.implicits._
    // Checkpoint 1

    val aadharDataPath = args(0)

    val aadharDataSchema = StructType(
    Seq(
    StructField("date", StringType),
    StructField("registrar", StringType),
    StructField("private_agency", StringType),
    StructField("state", StringType),
    StructField("district", StringType),
    StructField("sub_district", StringType),
    StructField("pincode", StringType),
    StructField("gender", StringType),
    StructField("age", IntegerType),
    StructField("aadhar_generated", IntegerType),
    StructField("rejected", IntegerType),
    StructField("mobile_number", IntegerType),
    StructField("email_id", IntegerType)
    )
    )

    val aadharDataRaw = spark.read.format("csv").schema(aadharDataSchema).load(aadharDataPath)

    aadharDataRaw.show()
    
    // describing dataframe to check summary statistics and see if there are null for numeric values
    aadharDataRaw.describe().show()

    val aadharData = aadharDataRaw.na.fill(0, Seq("mobile_number", "email_id"))

    // Checkpoint 2

    // 1. Describe the schema.

    aadharData.printSchema

    // 2. Find the count and names of registrars in the table.

    val registrarDF = aadharData.select("registrar").distinct
    registrarDF.count // 69
    registrarDF.show 

    // 3. Find the number of states, districts in each state and sub-districts in each district.

    // Number of states
    aadharData.select("state").distinct.count

    // Number of districts in each state.




    // number of private_agencies in each state
    aadharData.groupBy("state").agg(countDistinct("private_agency").as("cnt_private_agencies")).show

    // Checkpoint 3

    // 1. Find top 3 states generating most number of Aadhaar cards?

    aadharData.groupBy("state").agg(sum("aadhar_generated").as("sum_aadhar_generated")).orderBy($"sum_aadhar_generated".desc).show(3)


    // 2. Find top 3 districts where enrolment numbers are maximum?

     aadharData.groupBy("district").agg(sum($"aadhar_generated" + $"rejected").as("sum_aadhar_enrolled")).orderBy($"sum_aadhar_enrolled".desc).show(3)

     // 3. Find the no. of Aadhaar cards generated in each state?

      aadharData.groupBy("state").agg(sum("aadhar_generated").as("cnt_aadhar_generated")).orderBy($"cnt_aadhar_generated".desc)show(37)


      // Checkpoint 4

      // 1. Find the number of unique pincodes in the data?

       aadharData.select("pincode").distinct.count

       // 2. Find the number of Aadhaar registrations rejected in Uttar Pradesh and Maharashtra?

       aadharData.filter($"state" isin ("Maharashtra", "Uttar Pradesh")).groupBy("state").agg(sum($"rejected")).as("cnt_rejected").show

       // Checkpoint 5

       // 1. Find the top 3 states where the percentage of Aadhaar cards being generated for males is the highest.

    val stateAddGen = aadharData.groupBy("state").agg(sum($"aadhar_generated").as("tot_aadhar_generated"),
    sum(when($"gender" === "M", $"aadhar_generated").otherwise(lit(0))).as("tot_male_aad_gen"))
    stateAddGen.withColumn("male_perc", $"tot_male_aad_gen" / $"tot_aadhar_generated" * 100).orderBy(($"male_perc").desc).show(3)


    // 2. Find in each of these 3 states, identify the top 3 districts where the percentage of Aadhaar cards being rejected for females is the highest.

    val stateAddGenF = aadharData.filter($"state" isin ("Manipur", "Arunanchal Pradesh", "Nagaland")).groupBy("district").agg(sum($"rejected").as("total_rejected"),
    sum(when($"gender" === "F", $"rejected").otherwise(lit(0))).as("total_female_rejected"))

    stateAddGenF.withColumn("femalePercentage", $"total_female_rejected" / $"total_rejected" * 100).orderBy($"femalePercentage".desc).show(3)

    // 3. Find the summary of the acceptance percentage of all the Aadhaar cards applications by bucketing the age group into 10 buckets.


    // checkpoint 5

    val ageBucketizer = new Bucketizer().
    setInputCol("age").
    setOutputCol("age_bucket").
    setSplits( Range.Double(0,220,20).toArray )

    val aadharDataBinned = ageBucketizer.transform(aadharData)

    aadharDataBinned.groupBy("age_bucket").agg(
    sum($"aadhar_generated" + $"rejected").as("tot_requests"),
    sum($"aadhar_generated").as("tot_gen")
    ).withColumn("acceptance", $"tot_gen" / $"tot_requests" * lit(100)).show()
  }
}

