package com.example

import com.typesafe.config.{Config, ConfigFactory}
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
    val spark = SparkSession.builder
      .appName("RunSpark Application")
      .getOrCreate()

    val config : Config = ConfigFactory.parseResources("application.conf")

    val inputPath = config.getString("config.inputPath")
    val outputPath = config.getString("config.outputPath")

    val parametersSchema = Encoders.product[Parameters].schema
    val df = spark.read.schema(parametersSchema).option("header", true).csv(inputPath)
    df.persist

    df.createOrReplaceTempView("df")

    val sortedDf = spark.sql("SELECT * FROM df ORDER BY shortCountryCode, timestamp")

    sortedDf.repartition(1).write.format("csv").mode(SaveMode.Overwrite).save(outputPath)

  }

}
