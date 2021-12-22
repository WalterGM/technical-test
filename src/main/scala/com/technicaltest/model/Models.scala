package com.technicaltest.model

object Models {

  case class factTable(
      id: Integer,
      companyId: Integer,
      dateId: Integer,
      sectorId: Integer,
      titleId: Integer,
      applicantAgeRange: Integer,
      applicationDateId: Integer,
      countPerCompany: Long,
      countPerSector: Long)

  case class companyDimension(
      id: Integer,
      company: String,
      city: String)

  case class sectorDimension(
      sectorId: Integer,
      sector: String)

  case class timeDimension(
      dateId: Integer,
      day: Integer,
      month: Integer,
      year: Integer)

  case class Adverts(
      activeDays: Long,
      applyUrl: String,
      id: String,
      publicationDateTime: String,
      status: String)

  case class Applicants(
      age: Long,
      applicationDate: String,
      firstName: String,
      lastName: String,
      skills: Array[String])

  case class jobData(
      adverts: Adverts,
      applicants: Array[Applicants],
      benefits: Array[String],
      city: String,
      company: String,
      id: String,
      sector: String,
      title: String)

  case class TimeDim(
      DateNum: Integer,
      Date: String,
      YearMonthNum: Integer,
      Calendar_Quarter: String,
      MonthNum: Integer,
      MonthName: String,
      MonthShortName: String,
      WeekNum: Integer,
      DayNumOfYear: Integer,
      DayNumOfMonth: Integer,
      DayNumOfWeek: Integer,
      DayName: String,
      DayShortName: String,
      Quarter: String,
      YearQuarterNum: Integer,
      DayNumOfQuarter: Integer)
}
