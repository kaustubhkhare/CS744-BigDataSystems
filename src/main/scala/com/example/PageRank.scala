package com.example

import org.apache.log4j.{Level, LogManager}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}
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
      .config("spark.local.dir", "/mnt/data/tmp/")
      .getOrCreate()

    // Take paths from the user as command line arguments
    val inputPath = args(0)
    val outputPath = args(1)
    val persistData = "true".equals(args(2).toLowerCase)
    val partitionByCol: String = args(3)
    val partitionByNum: Int = args(4).toInt

    log.info(s"Loaded config inputPath->$inputPath, outputPath->$outputPath, persistData->$persistData, " +
      s"partitionByCol->$partitionByCol, partitionByNum->$partitionByNum")

    // Create schema object to read the data
    val linkSchema = Encoders.product[Links].schema

    log.info("Reading data into df")
    // Read csv data into the dataframe
    var df = spark.read.schema(linkSchema).option("header", false).option("delimiter", "\t").csv(inputPath)

    import spark.implicits._
    df = df
      .filter(r => {
        r != null && r.getString(0) != null && r.getString(1) != null
      })
      .map(r => (r.getString(0).toLowerCase().trim, r.getString(1).toLowerCase().trim)) // Trim any whitespaces before or after the values
      .filter(r => r._2.startsWith("category:") || !r._2.contains(":"))
      .dropDuplicates()
      .toDF("fromNode", "toNode")

    df.createOrReplaceTempView("df") // Create a view to be used in spark.sql

    log.info("Creating links")
    // l is number of links going out of a fromLink
    val links = spark.sql("select A.fromNode, A.toNode, B.l from df A join " +
      "(select fromNode, count(fromNode) l from df group by fromNode) B on A.fromNode = B.fromNode")

    if (persistData)
      links.persist(StorageLevel.MEMORY_ONLY) // Cache data to MEMORY and DISK

    // Create partitioning by fromNode and toNode columns
    if (partitionByNum == -1)
    links.repartition(col(partitionByCol))
    else
    links.repartition(partitionByNum)

    links.createOrReplaceTempView("links") // Create a view to be used in spark.sql

    log.info("Creating ranks")
    // Create the initial ranks dataframe with all ranks as 1
    var ranks = spark.sql("select distinct node, 1 as rank from " +
      "(select * from (select fromNode as node from links) union (select toNode as node from links))")
    ranks.repartition(col("node"))
    ranks.createOrReplaceTempView("ranks")

    val iterations = 10

    log.info(s"Starting for loop")
    for (i <- 1 to iterations) {
      log.info(s"Creating intmRanks ${i}")
      // Calculate modified ranks
      var intmRanks = spark.sql("select A.toNode as node, 0.15 + 0.85 * sum(B.rank / A.l) as rank from links A " +
        "right join ranks B on A.fromNode = B.node group by A.toNode")
      intmRanks.createOrReplaceTempView("intmRanks")

      log.info(s"Creating ranks ${i}")
      // Take all ranks modified and unmodified
      ranks = spark.sql("select coalesce(A.node, B.node) as node, coalesce(A.rank, B.rank) as rank " +
        "from intmRanks A right join ranks B on A.node = B.node")

      log.info(s"Creating ranks view ${i}")
      // Create partitioning by fromNode and toNode columns
      ranks.repartition(col("node"))
      ranks.createOrReplaceTempView("ranks") // Create a view to be used in spark.sql
    }

    log.info(s"Writing to output ${outputPath}")

    val finalRanks = spark.sql("select node, rank from ranks A join (select distinct fromNode from links) B on A.node = B.fromNode")

    // Overwrite mode overwrites any older output which might have been present
    finalRanks.write.mode(SaveMode.Overwrite).option("headers", true).csv(outputPath)

  }

}
