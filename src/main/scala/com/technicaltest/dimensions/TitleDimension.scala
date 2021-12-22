package com.technicaltest.dimensions

import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, SparkSession}

object TitleDimension {

  def apply(spark: SparkSession)(jobsDataFrame: DataFrame): DataFrame = {

    val titleDf = jobsDataFrame
      .select(col("title"))
      .distinct()

    titleDf.withColumn("idTitle", monotonically_increasing_id() + 1)

  }

}
