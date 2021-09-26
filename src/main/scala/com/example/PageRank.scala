package com.example

import com.typesafe.config.{Config, ConfigFactory}
import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Encoders, SaveMode, SparkSession}
import org.apache.spark.storage.StorageLevel

object PageRank {

  case class Links(fromNode: String,
                   toNode: String)

  def main(args: Array[String]): Unit = {
    val log = LogManager.getRootLogger()
    log.setLevel(Level.INFO)
    log.info("Building spark session")
    val spark = SparkSession.builder
      .appName("PageRank Application")
      .config("spark.local.dir","/mnt/data/tmp/")
      .getOrCreate()

    val config: Config = ConfigFactory.parseResources("application.conf")
    val inputPath = config.getString("config.pageRank.inputPath")
    val outputPath = config.getString("config.pageRank.outputPath")

    log.info(s"Loaded config inputPath->$inputPath, outputPath->$outputPath")

    log.info("Reading data into df")
    val linkSchema = Encoders.product[Links].schema
    var df = spark.read.schema(linkSchema).option("header", false).option("delimiter", "\t").csv(inputPath)
    df.createOrReplaceTempView("df")
    import spark.implicits._
    df = df.filter(r => {r == null || r.getString(0) == null || r.getString(1) == null})
      .map(r => (r.getString(0).trim, r.getString(1).trim)).toDF("fromNode", "toNode")

    log.info("Creating links")
    // l is number of links going out of a fromLink
    var links = spark.sql("select A.fromNode, A.toNode, B.l from df A join " +
      "(select fromNode, count(fromNode) l from df group by fromNode) B on A.fromNode = B.fromNode")
    links.persist()
    links.createOrReplaceTempView("links")
    links.repartition(col("fromNode"), col("toNode"))

    println("Creating ranks")
    var ranks1 = spark.sql("select distinct node, 1 as rank from " +
      "(select * from (select fromNode as node from links) union (select toNode as node from links))")
    ranks1.createOrReplaceTempView("ranks1")
    ranks1.repartition(col("node"))

    var ranks2: DataFrame = null
    var tmp: DataFrame = null

    val iterations = 2
    //  TODO: Create termination condition instead of loop

    log.info(s"Starting for loop")
    for (i <- 1 to iterations) {
      log.info(s"Creating intmRanks ${i}")
      val intmRanks2 = spark.sql("select A.toNode as node, 0.14 + 0.85 * sum(B.rank / A.l) as rank from links A " +
        "right join ranks1 B on A.fromNode = B.node group by A.toNode")
      intmRanks2.createOrReplaceTempView("intmRanks2")

      log.info(s"Creating ranks ${i}")
      ranks1 = spark.sql("select coalesce(A.node, B.node) as node, coalesce(A.rank, B.rank) as rank from intmRanks2 A right join ranks1 B on A.node = B.node")

      log.info(s"Creating ranks view ${i}")
      ranks1.createOrReplaceTempView("ranks1")
      ranks1.repartition(col("node"))
    }

    log.info(s"Writing to output ${outputPath}")
    ranks1.write.mode(SaveMode.Overwrite).option("headers", true).csv(outputPath)

  }

}
