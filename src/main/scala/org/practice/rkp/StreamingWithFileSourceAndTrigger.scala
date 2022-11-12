package org.practice.rkp

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.expr
import org.apache.spark.sql.streaming.Trigger

object StreamingWithFileSourceAndTrigger extends  Serializable {
  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args:Array[String]):Unit={

    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local[2]")
    sparkConf.set("spark.app.name", "streamingWordCount")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.sql.shuffle.partitions", "2")
    sparkConf.set("spark.sql.streaming.schemaInference","true")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val streamDf= spark.readStream
      .format("json")
      .option("path","input/fileSource")
      .option("maxFilesPerTrigger",1)
      .load

   //streamDf.printSchema()

  val formattedDf=  streamDf.selectExpr("CashierID","CreatedTime","CustomerCardNo","CustomerType",
    "DeliveryType","InvoiceNumber","NumberOfItems","PaymentMethod","PosID","StoreID"
    ,"DeliveryAddress.ContactNumber","explode(InvoiceLineItems) as lineItem")

    //formattedDf.printSchema()
   val finalDf= formattedDf.withColumn("ItemCode",expr("lineItem.ItemCode"))
    .withColumn("ItemDescription",expr("lineItem.ItemDescription"))
    .withColumn("ItemPrice",expr("lineItem.ItemPrice"))
    .withColumn("ItemQty",expr("lineItem.ItemQty"))
    .withColumn("TotalValue",expr("lineItem.TotalValue"))
     .drop("lineItem")

    //finalDf.printSchema()

   val dataStreamWriter= finalDf.writeStream
      .format("json")
      .outputMode("append")
      .option("path","output/fileSource")
      .option("checkpointLocation","check-points/fileSource")
      .trigger(Trigger.ProcessingTime("1 minute"))
      .queryName("Flatten writer")
      .start()


    dataStreamWriter.awaitTermination()

  }

}
