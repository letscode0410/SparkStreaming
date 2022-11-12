package org.practice.rkp

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr

object WordCountStreaming extends  Serializable {

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args:Array[String]):Unit={

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master","local[2]")
    sparkConf.set("spark.app.name","streamingWordCount")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown","true")
    sparkConf.set("spark.sql.shuffle.partitions","2")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val inputDF=spark.readStream
      .format("socket")
      .option("host","localhost")
      .option("port","9999")
      .load

    inputDF.printSchema()


   // val parseInputDF=inputDF.selectExpr("explode(split(value,' ')) as word")
    val parseInputDF=inputDF.select(expr("explode(split(value,' ')) as word"))
    val outputDF = parseInputDF.groupBy("word").count()



    val dataStreamWriter=outputDF.writeStream
      .format("console")
      .outputMode("complete")
      .option("checkpointLocation","checkpoint-location-wc")
      .start()

    dataStreamWriter.awaitTermination()


  }

}
