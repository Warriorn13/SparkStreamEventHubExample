package com.microsoft.cccvp.test

import java.nio.charset.StandardCharsets

import org.apache.spark._
import org.apache.spark.streaming._
import org.apache.spark.SparkContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.eventhubs.common.EventHubsUtils
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg,count}
import org.apache.spark.sql.types._

object StreamingWordCount {

  def main(args: Array[String]): Unit = {

    if (args.length != 6) {
      println("Usage: program progressDir PolicyName PolicyKey EventHubNamespace EventHubName" +
        " BatchDuration(seconds)")
      sys.exit(1)
    }

    val progressDir = args(0)
    val policyName = args(1)
    val policyKey = args(2)
    val eventHubNamespace = args(3)
    val eventHubName = args(4)
    val batchDuration = args(5).toInt

    val spark = SparkSession.builder.getOrCreate()

    val eventhubParameters = Map[String, String](
      "eventhubs.policyname" -> policyName,
      "eventhubs.policykey" -> policyKey,
      "eventhubs.namespace" -> eventHubNamespace,
      "eventhubs.name" -> eventHubName,
      "eventhubs.partition.count" -> "2",
      "eventhubs.consumergroup" -> "$Default"
    )

    val ssc = new StreamingContext(spark.sparkContext, Seconds(batchDuration))

    val schema = new StructType()
      .add("id", StringType)
      .add("timestamp", StringType)
      .add("uv", DoubleType)
      .add("temperature", IntegerType)
      .add("humidity", IntegerType)

    val inputDirectStream = EventHubsUtils.createDirectStreams(
      ssc,
      progressDir,
      Map(eventHubName -> eventhubParameters)).window(Durations.seconds(30))

    inputDirectStream.foreachRDD { (rdd, time) =>
      val data = rdd.map(record => new String(record.getBytes, StandardCharsets.UTF_8))
      val json = spark.read.schema(schema).json(data)
      json.createOrReplaceTempView("iot")
      json.sqlContext.sql("select id, avg(temperature) as avg_temperature, count(*) as count from iot group by id").show()
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
