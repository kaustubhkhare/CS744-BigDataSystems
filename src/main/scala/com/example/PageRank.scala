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
    val df = spark.read.schema(linkSchema).option("header", false)
      .option("delimiter", "\t").csv(inputPath)
    df.createOrReplaceTempView("df")

    // l is number of links going out of a fromLink
    var links = spark.sql("select A.fromNode, A.toNode, B.l from df A join " +
      "(select fromNode, count(fromNode) l from df group by fromNode) B on A.fromNode = B.fromNode")
    //    links.persist()

    var ranks1 = spark.sql("select distinct node, 1 as rank from (select * from (select fromNode as node from df) union (select toNode as node from df))")
    var ranks2: DataFrame = null
    var tmp: DataFrame = null

    links.createOrReplaceTempView("links")
    ranks1.createOrReplaceTempView("ranks1")

    val iterations = 10
    //  TODO: Create termination condition instead of loop
    for (_ <- 1 to iterations) {
      val intmRanks2 = spark.sql("select A.toNode as node, 0.14 + 0.85 * sum(B.rank / A.l) as rank from links A " +
        "right join ranks1 B on A.fromNode = B.node group by A.toNode")
      intmRanks2.createOrReplaceTempView("intmRanks2")

      ranks2 = spark.sql("select coalesce(A.node, B.node) as node, coalesce(A.rank, B.rank) as rank from intmRanks2 A right join ranks1 B on A.node = B.node")

      tmp = ranks1
      ranks1 = ranks2
      ranks2 = tmp
      ranks1.createOrReplaceTempView("ranks1")
    }

    ranks1.write.option("headers", true).csv(outputPath)

  }

}
