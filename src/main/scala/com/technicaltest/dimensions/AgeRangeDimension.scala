package com.technicaltest.dimensions

import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object AgeRangeDimension {

  def apply(spark: SparkSession)(jobs: DataFrame): DataFrame = {

    val applicantsDf = jobs
      .select(col("id"), col("applicants"))
      .withColumn("applicants_exploded", explode_outer(col("applicants")))
      .drop(col("applicants"))
      .select(
        col("id"),
        col("applicants_exploded.age").as("age"),
        col("applicants_exploded.applicationDate").as("applicationDate")
      )
      .distinct()

    applicantsDf
      .withColumn("applicantId", monotonically_increasing_id() + 1)
      .withColumn(
        "age_lower_than_25",
        when(col("age") <= 25, lit(1))
          .otherwise(lit(0))
      )
      .withColumn(
        "age_between_26_35",
        when(col("age") >= 26 && col("age") <= 35, lit(1))
          .otherwise(lit(0))
      )
      .withColumn(
        "age_between_36_45",
        when(col("age") >= 36 && col("age") <= 45, lit(1))
          .otherwise(lit(0))
      )
      .withColumn(
        "age_greater_than_45",
        when(col("age") >= 46, lit(1))
          .otherwise(lit(0))
      )
  }
}
