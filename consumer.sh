#!/usr/bin/env bash
/usr/local/spark/bin/spark-submit --verbose --master local --class \
com.awsapache.streamsql.MainSparkStreaming \
target/streamsql-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
localhost:9092 topictest jdbc:redshift://52.77.123.232:5439/telenorhealth?user=admin&password=Telenor!23 s3n://redshift-temp-spark/data