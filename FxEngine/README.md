###### Refinitiv Matching Engine Exercise

Your task is to create a new matching engine for FX orders. The engine will take a CSV file of orders for a given
currency pair and match orders together. In this example you'll be looking at USD/GBP.

There are two types of orders, BUY and SELL orders. A BUY order is for the price in USD you'll pay for GBP, SELL
order is the price in USD you'll sell GBP for.

Each order has the following fields:
1. org.example.Order ID
        - This is a unique ID in the file which is used to track an order
2. User Name
        - This is the user name of the user making the order
3. org.example.Order Time
        - This is the time, in milliseconds since Jan 1st 1970, the order was placed
4. org.example.Order Type
        - Either BUY or SELL
5. Quantity
        - The number of currency units you want to BUY or SELL
6. Price
        - The price you wish to sell for, this is in the lowest unit of the currency, i.e. for GBP it's in pence and for USD it's cents

The matching engine must do the following:
- It should match orders when they have the same quantity
- If an order is matched it should be closed
- If an order is not matched it should be kept on an "order book" and wait for an order which does match
- When matching an order to the book the order should look for the best price
- The best price for a BUY is the lowest possible price to BUY for
- The best price for a SELL is the highest possible price to SELL for
- You should always use the price from the "order book" when matching orders
- When an order has matched you should record the IDs of both orders, the price, quantity and time of the match
- If two orders match with the same price the first order is used
- Orders won't have the same timestamp

The file exampleOrders.csv is some example trading data, the matches for that trading data is in outputExampleMatches.csv



<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>manika</groupId>
    <artifactId>TradeEngine</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>20</maven.compiler.source>
        <maven.compiler.target>20</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <dependencies>
    <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-core_2.13</artifactId>
        <version>3.4.0</version>
    </dependency>
        <dependency>
        <groupId>org.apache.spark</groupId>
        <artifactId>spark-sql_2.13</artifactId>
        <version>3.4.0</version>
        <scope>provided</scope>
    </dependency>
    </dependencies>

</project>


import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructField, StructType}

object ABC {
  def main(args: Array[String]): Unit = {
    println("Hello world!")
    val spark = SparkSession.builder()
      .appName("FXOrderMatchingEngine")
      .master("local[*]")
      .getOrCreate()

    val schema = StructType(
      Array(
        StructField("OrderID", StringType, nullable = true),
        StructField("UserName", StringType, nullable = true),
        StructField("OrderTime", LongType, nullable = true),
        StructField("OrderType", StringType, nullable = true),
        StructField("Quantity", IntegerType, nullable = true),
        StructField("Price", IntegerType, nullable = true)
      )
    )
    // Read the CSV file of orders
    val ordersDF = spark.read
      .option("header", "true")
      .option("inferSchema", "false")
      .schema("customSchema")
      .csv("coding_exercise/exampleOrders.csv")

    val convertedDF = ordersDF.withColumn("OrderTime", from_unixtime(col("OrderTime")))
    convertedDF.show()

  }
}
