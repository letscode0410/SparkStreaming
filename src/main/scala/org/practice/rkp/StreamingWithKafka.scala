package org.practice.rkp

import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, expr, from_json}
import org.apache.spark.sql.types.{ArrayType, DoubleType, IntegerType, LongType, StringType, StructField, StructType}

object StreamingWithKafka extends Serializable {

  @transient lazy val logger:Logger = Logger.getLogger(getClass.getName)

  def main(args:Array[String]):Unit= {
    val schema = StructType(List(
      StructField("InvoiceNumber", StringType),
      StructField("CreatedTime", LongType),
      StructField("StoreID", StringType),
      StructField("PosID", StringType),
      StructField("CashierID", StringType),
      StructField("CustomerType", StringType),
      StructField("CustomerCardNo", StringType),
      StructField("TotalAmount", DoubleType),
      StructField("NumberOfItems", IntegerType),
      StructField("PaymentMethod", StringType),
      StructField("CGST", DoubleType),
      StructField("SGST", DoubleType),
      StructField("CESS", DoubleType),
      StructField("DeliveryType", StringType),
      StructField("DeliveryAddress", StructType(List(
        StructField("AddressLine", StringType),
        StructField("City", StringType),
        StructField("State", StringType),
        StructField("PinCode", StringType),
        StructField("ContactNumber", StringType)
      ))),
      StructField("InvoiceLineItems", ArrayType(StructType(List(
        StructField("ItemCode", StringType),
        StructField("ItemDescription", StringType),
        StructField("ItemPrice", DoubleType),
        StructField("ItemQty", IntegerType),
        StructField("TotalValue", DoubleType)
      )))),
    ))


    val sparkConf = new SparkConf()
    sparkConf.set("spark.master", "local[2]")
    sparkConf.set("spark.app.name", "streaming with kafka")
    sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true")
    sparkConf.set("spark.sql.shuffle.partitions", "2")

    val spark = SparkSession
      .builder()
      .config(sparkConf)
      .getOrCreate()

    val streamingDf = spark.readStream
      .format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "invoice")
      .option("startingOffsets", "earliest")
      .load

    //streamingDf.printSchema()
    val structuredDf=streamingDf.select(from_json(col("value").cast("string"),schema).alias("value"))

    val formattedDf=structuredDf.selectExpr("value.InvoiceNumber",
    "value.CreatedTime",
    "value.StoreID",
    "value.PosID",
    "value.CustomerType",
    "value.PaymentMethod",
    "value.DeliveryType",
    "value.DeliveryAddress.City",
    "value.DeliveryAddress.State",
    "value.DeliveryAddress.PinCode",
    "explode(value.InvoiceLineItems) as LineItem")

    val finalDf = formattedDf.withColumn("ItemCode", expr("lineItem.ItemCode"))
      .withColumn("ItemDescription", expr("lineItem.ItemDescription"))
      .withColumn("ItemPrice", expr("lineItem.ItemPrice"))
      .withColumn("ItemQty", expr("lineItem.ItemQty"))
      .withColumn("TotalValue", expr("lineItem.TotalValue"))
      .drop("lineItem")

    val dataStreamWriter = finalDf.writeStream
      .format("json")
      .outputMode("append")
      .option("path", "output/fileSource")
      .option("checkpointLocation", "check-points/fileSource")
      .queryName("Flatten writer")
      .start()


    dataStreamWriter.awaitTermination()

  }
}
