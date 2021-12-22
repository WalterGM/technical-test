package com.technicaltest.fact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, count, sum}

object JobFactTable {

  def apply(
      sectorDimension: DataFrame,
      companyDimension: DataFrame,
      ageRangeDimension: DataFrame,
      titleDimension: DataFrame,
      publicationDateDimension: DataFrame
    )(
      jodData: DataFrame
    ): DataFrame = {

    val factDataFrame = jodData
      .as("job")
      .select(col("job.id"), col("job.title"), col("job.company"), col("job.sector"))
      .join(titleDimension.as("t"), col("t.title") === col("job.title"), "inner")
      .join(companyDimension.as("c"), col("c.company") === col("job.company"), "inner")
      .join(sectorDimension.as("s"), col("s.sector") === col("job.sector"), "inner")
      .join(ageRangeDimension.as("a"), col("a.id") === col("job.id"), "inner")
      .join(publicationDateDimension.as("p"), col("p.id") === col("job.id"), "inner")
    factDataFrame
      .select(
        col("job.id"),
        col("idTitle"),
        col("idCompany"),
        col("sectorId"),
        col("a.age_lower_than_25"),
        col("a.age_between_26_35"),
        col("a.age_between_36_45"),
        col("a.age_greater_than_45"),
        col("publicationDateTime")
      )
      .groupBy(
        col("idTitle"),
        col("idCompany"),
        col("sectorId"),
        col("publicationDateTime")
      )
      .agg(
        count(col("job.id")),
        sum(col("a.age_lower_than_25")),
        sum(col("a.age_between_26_35")),
        sum(col("a.age_between_36_45")),
        sum(col("a.age_greater_than_45"))
      )

  }

}
