#!/usr/bin/env bash
/usr/local/spark/bin/spark-submit --verbose --master local --class \
com.awsapache.streamsql.MainSparkStreaming --jars \
streamsql-0.0.1-SNAPSHOT-jar-with-dependencies.jar \
test kafka1.data-insights.telenordigital.com:9093 toniccore-db