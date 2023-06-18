package tcs.assessment.code

import org.apache.spark.sql.functions.{asc, desc}
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object TradeEngine {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local")
      .appName("RefinitivMatchingEngine")
      .getOrCreate()

    // hide _SUCCESS files in output
    spark.conf.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    spark.sparkContext.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")

    import spark.implicits._

    val schema = Encoders.product[Order].schema

    //val inputOrderPath = args(0)
    val inoutOrdersDF = spark.read
      .format("csv")
      .schema(schema)
      //.load(inputOrderPath)
      .load("input/inputOrders.csv")

    inoutOrdersDF.show()

    val inputDS = inoutOrdersDF.as[Order]
    var orderBookDS = spark.createDataset(Seq.empty[Order])
    var matchedDS = spark.createDataset(Seq.empty[Match])

    inputDS.take(inputDS.count().toInt).foreach(order => {
      val typeOfMatchingOrder = if (order.orderType == "BUY") "SELL" else "BUY"
      val quantity = order.quantity
      var matchingOrders = orderBookDS.filter($"orderType" === typeOfMatchingOrder && $"quantity" === quantity)

      // find best price in matchingOrders
      matchingOrders = if (order.orderType == "SELL") matchingOrders.orderBy(desc("price")) // highest buy price
      else matchingOrders.orderBy(asc("price")) // lowest sell price
      if (!matchingOrders.isEmpty) {
        val matchedOrder = matchingOrders.first()
        println(matchedOrder)
        val output = Match(order.orderId, matchedOrder.orderId, order.orderTime, quantity, matchedOrder.price)
        matchedDS = matchedDS.union(Seq(output).toDS())
        orderBookDS = orderBookDS.filter($"orderId" =!= matchedOrder.orderId)
      }
      else {
        orderBookDS = orderBookDS.union(Seq(order).toDS())
      }
      orderBookDS.show()
    })

    matchedDS.show()
    matchedDS.repartition(1).write.option("header", value = true)
      .mode(SaveMode.Overwrite)
      .csv("output/matchedOrders.csv")
    orderBookDS.repartition(1).write.option("header", value = true)
      .mode(SaveMode.Overwrite)
      .csv("output/OpenOrders.csv")
  }
}
