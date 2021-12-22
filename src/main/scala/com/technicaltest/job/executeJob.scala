package com.technicaltest.job

import com.technicaltest.dimensions._
import com.technicaltest.fact.JobFactTable
import com.technicaltest.model.Models.jobData
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.types.StructType

object executeJob {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder
      .master("local[*]")
      .appName("Execute Model")
//      .config("spark.hadoop.fs.s3a.access.key","AKIA3FHPVOOHL5GJMHWN")
//      .config("spark.hadoop.fs.s3a.secret.key","Co8fcHKuwf+L0e2tsalQETqSaE9SfUjv+PkVEIq")
//      .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
      .getOrCreate()

    val schemaFile =
      ScalaReflection.schemaFor[jobData].dataType.asInstanceOf[StructType]

    val df = spark.read
      .option("multiline", "true")
      .schema(schemaFile)
      .json("src/main/resources/common/time_dimension/data-sample.json")

    val titleDimension           = df.transform(TitleDimension(spark))
    val companyDimension         = df.transform(CompanyDimension(spark))
    val sectorDimension          = df.transform(SectorDimension(spark))
    val publicationDateDimension = df.transform(PublicationDateDimension(spark))
    val ageRangeDimension        = df.transform(AgeRangeDimension(spark))
    val timeDimension            = TimeDimension(spark)

    val factTable = df.transform(
      JobFactTable(
        sectorDimension,
        companyDimension,
        ageRangeDimension,
        titleDimension,
        publicationDateDimension
      )
    )

  }

}
