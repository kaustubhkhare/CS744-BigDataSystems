package com.example

import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object Assignment1 {

  case class Parameters(batteryLevel: Int,
                        c02Level: Int,
                        shortCountryCode: String,
                        longCountryCode: String,
                        countryName: String,
                        deviceId: Int,
                        deviceName: String,
                        humidity: Int,
                        ip: String,
                        latitude: Double,
                        lcd: String,
                        longitude: Double,
                        tempScale: String,
                        temp: Double,
                        timestamp: Long)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder.appName("RunSpark Application").getOrCreate()
    val inputPath = args(0)
    val outputPath = args(1)

    val parametersSchema = Encoders.product[Parameters].schema
    var df = spark.read.schema(parametersSchema).option("header", true).csv(inputPath)
    df.persist

    df.createOrReplaceTempView("df")

    var sortedDf = spark.sql("SELECT * FROM df ORDER BY shortCountryCode, timestamp")

    sortedDf.write.format("csv").mode(SaveMode.Overwrite).save(outputPath)

  }

}
