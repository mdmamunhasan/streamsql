/*
Command for execution
/usr/local/spark/bin/spark-submit --verbose --master local --class \
com.awsapache.streamsql.MainSparkStreaming \
target/streamsql-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
localhost:9092 topictest host:port database user password
*/

package com.awsapache.streamsql


import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.streaming.kafka._
import org.apache.spark.streaming.{Seconds, StreamingContext, Time}
import com.amazonaws.auth._
import com.amazonaws.auth.AWSCredentialsProvider
import com.amazonaws.auth.AWSSessionCredentials
import com.amazonaws.auth.InstanceProfileCredentialsProvider
import com.amazonaws.services.redshift.AmazonRedshiftClient
import com.amazon.redshift.jdbc41.Driver

import scala.util.parsing.json._


object MainSparkStreaming {

  def main(args: Array[String]) {

    if (args.length < 6) {
      System.err.println(
        s"""
           |Usage: MainSparkStreaming <brokers> <topics>
           |  <brokers> is a list of one or more Kafka brokers
           |  <topics> is a list of one or more kafka topics to consume from
           |  <redshifthost> host:port
           |  <database> database name
           |  <db_user> databse username
           |  <db_password> database password
           |
        """.stripMargin)
      System.exit(1)
    }

    val Array(brokers, topics, redshifthost, database, db_user, db_password) = args

    val jdbcURL = "jdbc:redshift://" + redshifthost + "/" + database + "?user=" + db_user + "&password=" + db_password
    val tempS3Dir = "s3://redshift-temp-spark/data"

    val sparkConf = new SparkConf().setAppName("DirectKafkaActivities")
    // Create context with 10 second batch interval
    val ssc = new StreamingContext(sparkConf, Seconds(20))

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

    val provider = new InstanceProfileCredentialsProvider(false)
    val credentials: AWSSessionCredentials = provider.getCredentials.asInstanceOf[AWSSessionCredentials]
    val token = credentials.getSessionToken
    val awsAccessKey = credentials.getAWSAccessKeyId
    val awsSecretKey = credentials.getAWSSecretKey

    val retailerDF = spark.read
      .format("com.databricks.spark.redshift")
      .option("temporary_aws_access_key_id", awsSecretKey)
      .option("temporary_aws_secret_access_key", awsAccessKey)
      .option("temporary_aws_session_token", token)
      .option("url", jdbcURL)
      .option("dbtable", "retailer_invites")
      .option("aws_iam_role", sys.env("AWS_IAM_ROLE"))
      //.option("forward_spark_s3_credentials", true)
      .option("tempdir", tempS3Dir)
      .load()

    /*val retailerDF = spark.read
      .format("jdbc")
      .option("url", "jdbc:postgresql://"+redshifthost+"/"+database)
      .option("dbtable", "retailer_invites")
      .option("user", db_user)
      .option("password", db_password)
      .option("driver", "org.postgresql.Driver")
      .load()*/

    retailerDF.show()
    retailerDF.printSchema()

    // Drop the tables if it already exists 
    //spark.sql("DROP TABLE IF EXISTS retailer_invites_hive_table")

    // Create the tables to store your streams 
    //spark.sql("CREATE TABLE retailer_invites_hive_table ( retailernumber string, retailer_type string, msisdn string, requested_package string, invitedon string, status string, invite_type string ) STORED AS TEXTFILE")
    // Convert RDDs of the lines DStream to DataFrame and run SQL query
    lines.foreachRDD { (rdd: RDD[String], time: Time) =>

      import spark.implicits._
      // Convert RDD[String] to RDD[case class] to DataFrame

      //var z:Array[String] = new RDD[Map[String]]

      val messagesDataFrame = rdd.map(json => JSON.parseFull(json).get.asInstanceOf[Map[String, Any]])
      //messagesDataFrame.collect().foreach(println)

      // Creates a retailer DataFrame
      /*val retailerDataFrame = messagesDataFrame.filter(_ ("table") == "retailer_invites")
        .filter(_ ("operation") == "Insert").map(m => m("data").asInstanceOf[Map[String, String]])
        .map(w => RetailerRecord(w("retailernumber"), w("retailer_type"), w("msisdn"), w("requested_package"), w("invitedon"), w("status"), w("invite_type")))
        .toDF()*/

      // Creates a temporary view using the DataFrame
      //retailerDataFrame.createOrReplaceTempView("retailer_invites_messages")

      //Insert continuous streams into hive table
      //spark.sql("insert into table retailer_invites_hive_table select * from retailer_invites_messages")
      //spark.sql("insert into table retailer_invites(retailernumber, retailer_type, msisdn, requested_package, invitedon, status, invite_type) select * from retailer_invites_messages")
      /*spark.sql("INSERT INTO table retailer_invites(retailernumber, retailer_type, msisdn, requested_package, invitedon, status, invite_type) VALUES ('8801709496187', 'MassRetail', '8801785230660', 'TonicBasic', '2017-05-13 23:31:58', 'Pending', 'ADD_MEMBER')")
        .write.format("com.databricks.spark.redshift")
        .option("temporary_aws_access_key_id", awsSecretKey)
        .option("temporary_aws_secret_access_key", awsAccessKey)
        .option("temporary_aws_session_token", token)
        .option("url", jdbcURL)
        .option("dbtable", "retailer_invites")
        .option("aws_iam_role", sys.env("AWS_IAM_ROLE"))
        .mode(SaveMode.Append)
        .save()*/

      /*val retailerDF = spark.sql("INSERT INTO retailer_invites(retailernumber, retailer_type, msisdn, requested_package, invitedon, status, invite_type) VALUES ('8801709496187', 'MassRetail', '8801785230660', 'TonicBasic', '2017-05-13 23:31:58', 'Pending', 'ADD_MEMBER')")
        .write.format("jdbc")
        .option("url", "jdbc:postgresql://"+redshifthost+"/"+database)
        .option("dbtable", "retailer_invites")
        .option("user", db_user)
        .option("password", db_password)
        .option("driver", "org.postgresql.Driver")
        .save()*/

      // select the parsed messages from table using SQL and print it (since it runs on drive display few records)
      //val messagesqueryDataFrame = spark.sql("select * from retailer_invites_messages")
      println(s"========= $time =========")
      //messagesqueryDataFrame.show()
    }

    // Start the computation
    ssc.start()
    ssc.awaitTermination()

  }
}

/** Case class for converting RDD to DataFrame */
case class RetailerRecord(retailernumber: String, retailer_type: String, msisdn: String, requested_package: String, invitedon: String, status: String, invite_type: String)
