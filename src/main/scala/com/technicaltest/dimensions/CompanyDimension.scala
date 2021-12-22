package com.technicaltest.dimensions

import org.apache.spark.sql.functions.{col, monotonically_increasing_id}
import org.apache.spark.sql.{DataFrame, SparkSession}

object CompanyDimension {

  def apply(spark: SparkSession)(dataframeJobs: DataFrame): DataFrame = {

    val companyDf = dataframeJobs
      .select(col("company"), col("city"))
      .distinct()
      .repartition(1)
    companyDf.withColumn("idCompany", monotonically_increasing_id() + 1)
  }

}
