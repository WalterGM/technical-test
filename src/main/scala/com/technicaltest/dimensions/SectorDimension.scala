package com.technicaltest.dimensions

import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, SparkSession}

object SectorDimension {

  def apply(spark: SparkSession)(jobsDataFrame: DataFrame): DataFrame = {

    val sectorDf = jobsDataFrame
      .select(col("sector"))
      .distinct

    sectorDf.withColumn("sectorId", monotonically_increasing_id() + 1)
  }

}
