package org.example

import org.apache.spark.sql.functions.{col, expr, first, greatest}
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}
import org.apache.spark.sql.{SaveMode, SparkSession}

object SparkSessionTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    val schema = new StructType()
      .add("orderID", StringType, nullable = true)
      .add("userName", StringType, nullable = true)
      .add("orderTime", LongType, nullable = true)
      .add("orderType", StringType, nullable = true)
      .add("quantity", IntegerType, nullable = true)
      .add("price", IntegerType, nullable = true)

    val ordersDF = spark.read
      .format("csv")
      .schema(schema)
      .load("coding_exercise/input.csv")

    ordersDF.show()

    val buyOrdersDF = ordersDF.filter(col("orderType") === "BUY")
    val sellOrdersDF = ordersDF.filter(col("orderType") === "SELL")

    val matchedOrdersDF = sellOrdersDF.as("sell")
      .join(buyOrdersDF.as("buy"), expr("sell.quantity = buy.quantity"), "inner")
      .where(expr("sell.price <= buy.price"))
      .groupBy(col("sell.orderId").as("orderId1"))
      .agg(first(col("buy.orderId")).as("orderId2"),
        first(col("sell.price")).as("price"),
        first(col("sell.quantity")).as("quantity"),
        first(col("sell.orderTime")).as("matchTimeSell"),
        first(col("buy.orderTime")).as("matchTimeBuy"))

    val unmatchedBuyOrdersDF = buyOrdersDF
      .join(matchedOrdersDF, col("orderId") === col("orderId2"), "left_anti")
      .select(col("orderId"), col("userName"), col("orderTime"),
        col("orderType"), col("quantity"), col("price"))

    val unmatchedSellOrdersDF = sellOrdersDF
      .join(matchedOrdersDF, col("orderId") === col("orderId1"), "left_anti")
      .select(col("orderId"), col("userName"), col("orderTime"),
        col("orderType"), col("quantity"), col("price"))

    val orderBookDF = unmatchedBuyOrdersDF.union(unmatchedSellOrdersDF)

    val matchesDF = matchedOrdersDF.select(col("orderId2"), col("orderId1"),
      greatest(col("matchTimeSell"), col("matchTimeBuy")).as("matchTime"),
      col("quantity"), col("price"))

    println("org.example.Order book:")
    orderBookDF.show()

    println("Matches:")
    matchesDF.show()

    matchedOrdersDF.write.option("header", value = true)
      .mode(SaveMode.Overwrite)
      .csv("coding_exercise/output.csv")

  }
}

