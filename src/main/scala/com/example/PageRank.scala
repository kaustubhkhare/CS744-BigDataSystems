package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.sql.{DataFrame, Encoders, SparkSession}
import org.apache.spark.storage.StorageLevel

object PageRank {

  case class Links(fromNode: Int,
                   toNode: Int)

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder
      .appName("PageRank Application")
      .getOrCreate()

    val config: Config = ConfigFactory.parseResources("application.conf")

    val inputPath = config.getString("config.inputPath")
    val outputPath = config.getString("config.outputPath")

    val linkSchema = Encoders.product[Links].schema
    val df = spark.read.schema(linkSchema).option("header", true).csv(inputPath)
    df.createOrReplaceTempView("df")

    var links = spark.sql("select A.fromNode, A.toNode, B.l from df A join " +
      "(select fromNode, count(fromNode) l from df group by fromNode) B on A.fromNode = B.fromNode")
    links.persist()

    var ranks1 = spark.sql("select fromNode as node, 1 as rank from df group by fromNode")
    var ranks2: DataFrame = null
    var tmp: DataFrame = null

    links.createOrReplaceTempView("links")
    ranks1.createOrReplaceTempView("ranks1")


//  TODO: Create termination condition instead of loop
    for (_ <- 1 to 10) {
      ranks2 = spark.sql("select A.toNode as node, sum(B.rank / A.l) as rank from links A " +
        "join ranks1 B on A.fromNode = B.node group by A.toNode")

      tmp = ranks1
      ranks1 = ranks2
      ranks2 = tmp
      ranks1.createOrReplaceTempView("ranks1")
    }

  }

}
