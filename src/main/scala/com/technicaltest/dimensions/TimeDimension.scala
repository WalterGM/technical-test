package com.technicaltest.dimensions

import com.technicaltest.model.Models.TimeDim
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.functions.{col, to_date, year}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TimeDimension {

  def apply(spark: SparkSession): DataFrame = {

    val schemaFile =
      ScalaReflection.schemaFor[TimeDim].dataType.asInstanceOf[StructType]

    spark.read
      .schema(schemaFile)
      .option("header", "true")
      .csv("src/main/resources/common/time_dimension/dimdates.csv")
      .withColumn("year", year(to_date(col("dateNum").cast(StringType), "yyyyMMdd")))
  }
}
