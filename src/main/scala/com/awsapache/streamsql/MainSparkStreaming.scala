package com.awsapache.streamsql

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}


object MainSparkStreaming {

  def main(args: Array[String]) {

    if (args.length < 2) {
      System.err.println(
        s"""
           |Usage: MainSparkStreaming <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics) = args

    val sparkConf = new SparkConf().setAppName("DirectKafkaActivities")
    // Create context with 10 second batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(10))

    // Create direct kafka stream with brokers and topics
    val topicsSet = topics.split(",").toSet

    val kafkaParams = Map[String, String](
      "metadata.broker.list" -> brokers
    )

    val messages = KafkaUtils.createDirectStream[String, String, StringDecoder, StringDecoder](
      ssc, kafkaParams, topicsSet)

    val lines = messages.map(_._2)

    val warehouseLocation = "file:${system:user.dir}/spark-warehouse"

    val spark = SparkSession
      .builder
      .config(sparkConf)
      .config("spark.sql.warehouse.dir", warehouseLocation)
      .enableHiveSupport()
      .getOrCreate()

    // Drop the tables if it already exists 
    spark.sql("DROP TABLE IF EXISTS retailer_invites_hive_table")

    // Create the tables to store your streams 
    spark.sql("CREATE TABLE retailer_invites_hive_table ( retailernumber string, retailer_type string, msisdn string, requested_package string, invitedon string, status string, invite_type string ) STORED AS TEXTFILE")

    // Convert RDDs of the lines DStream to DataFrame and run SQL query
    lines.foreachRDD { (rdd: RDD[String], time: Time) =>

      import spark.implicits._
      // Convert RDD[String] to RDD[case class] to DataFrame

      val messagesDataFrame = rdd.map(_.split(",")).map(w => MemberRecord(w(0), w(1), w(2), w(3), w(4), w(5), w(6))).toDF()

      // Creates a temporary view using the DataFrame
      messagesDataFrame.createOrReplaceTempView("csmessages")

      //Insert continuous streams into hive table
      spark.sql("insert into table retailer_invites_hive_table select * from csmessages")

      // select the parsed messages from table using SQL and print it (since it runs on drive display few records)
      val messagesqueryDataFrame =
        spark.sql("select * from csmessages")
      println(s"========= $time =========")
      messagesqueryDataFrame.show()
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}

/** Case class for converting RDD to DataFrame */
case class MemberRecord(retailernumber: String, retailer_type: String, msisdn: String, requested_package: String, invitedon: String, status: String, invite_type: String)
