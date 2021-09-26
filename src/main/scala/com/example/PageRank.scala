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

    log.info(s"Loaded config inputPath->$inputPath, outputPath->$outputPath")

    log.info("Reading data into df")
    // Create schema object to read the data
    val linkSchema = Encoders.product[Links].schema
    // Read csv data into the dataframe
    var df = spark.read.schema(linkSchema).option("header", false).option("delimiter", "\t").csv(inputPath)
    df.createOrReplaceTempView("df") // Create a view to be used in spark.sql
    import spark.implicits._
    // Filtering out any null values
      df = df.filter(r => {
          r == null || r.getString(0) == null || r.getString(1) == null
    }) // Trim any whitespaces before or after the values
      .map(r => (r.getString(0).toLowerCase().trim, r.getString(1).toLowerCase().trim))
      .filter(r => r._2.contains("category:") || !r._2.contains(":"))
      .toDF("fromNode", "toNode")

    log.info("Creating links")
    // l is number of links going out of a fromLink
    val links = spark.sql("select A.fromNode, A.toNode, B.l from df A join " +
      "(select fromNode, count(fromNode) l from df group by fromNode) B on A.fromNode = B.fromNode")
    links.persist(StorageLevel.MEMORY_ONLY) // Cache data to MEMORY and DISK
    links.createOrReplaceTempView("links") // Create a view to be used in spark.sql

    // Create partitioning by fromNode and toNode columns
    links.repartition(col("fromNode"), col("toNode"))

    println("Creating ranks")
    // Create the initial ranks dataframe with all ranks as 1
    var ranks1 = spark.sql("select distinct node, 1 as rank from " +
      "(select * from (select fromNode as node from links) union (select toNode as node from links))")
    ranks1.createOrReplaceTempView("ranks1")
    ranks1.repartition(col("node"))

    val iterations = 10

    log.info(s"Starting for loop")
    for (i <- 1 to iterations) {
      log.info(s"Creating intmRanks ${i}")
      // Calculate modified ranks
      val intmRanks2 = spark.sql("select A.toNode as node, 0.14 + 0.85 * sum(B.rank / A.l) as rank from links A " +
        "right join ranks1 B on A.fromNode = B.node group by A.toNode")
      intmRanks2.createOrReplaceTempView("intmRanks2")

      log.info(s"Creating ranks ${i}")
      // Take all ranks modified and unmodified
      ranks1 = spark.sql("select coalesce(A.node, B.node) as node, coalesce(A.rank, B.rank) as rank from intmRanks2 A right join ranks1 B on A.node = B.node")

      log.info(s"Creating ranks view ${i}")
      ranks1.createOrReplaceTempView("ranks1") // Create a view to be used in spark.sql
      // Create partitioning by fromNode and toNode columns
      ranks1.repartition(col("node"))
    }

    log.info(s"Writing to output ${outputPath}")
    // Overwrite mode overwrites any older output which might have been present
    ranks1.write.mode(SaveMode.Overwrite).option("headers", true).csv(outputPath)

  }

}
