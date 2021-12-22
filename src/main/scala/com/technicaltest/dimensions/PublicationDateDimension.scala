package com.technicaltest.dimensions

import org.apache.spark.sql.functions.{col, date_format, from_unixtime}
import org.apache.spark.sql.types.IntegerType
import org.apache.spark.sql.{DataFrame, SparkSession}

object PublicationDateDimension {

  def apply(spark: SparkSession)(jobDataframe: DataFrame): DataFrame =
    jobDataframe
      .select(
        col("id"),
        col("adverts.id").as("advertsId"),
        col("adverts.publicationDateTime").as("publicationDateTime")
      )
      .distinct()
      .withColumn(
        "publicationDateTime",
        date_format(from_unixtime(col("publicationDateTime")), "yyyyMMdd")
          .cast(IntegerType)
      )

}
