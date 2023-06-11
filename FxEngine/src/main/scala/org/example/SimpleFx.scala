package org.example

import org.apache.spark.sql.functions.{asc, desc}
import org.apache.spark.sql.{Encoders, SaveMode, SparkSession}

object SimpleFx {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder()
      .master("local[1]")
      .appName("SparkByExample")
      .getOrCreate()

    import spark.implicits._

    val schema = Encoders.product[Order].schema

    val inoutOrdersDF = spark.read
      .format("csv")
      .schema(schema)
      .load("coding_exercise/input.csv")
    inoutOrdersDF.show()

    val inputDS = inoutOrdersDF.as[Order]
    var orderBookDS = spark.createDataset(Seq.empty[Order])
    var matchedDS = spark.createDataset(Seq.empty[Match])

    inputDS.take(inputDS.count().toInt).foreach(order => {
      val reqType = if (order.orderType == "BUY") "SELL" else "BUY"
      val quantity = order.quantity
      var matchingDS = orderBookDS.filter($"orderType" === reqType && $"quantity" === quantity)
      matchingDS = if (reqType == "BUY") matchingDS.orderBy(desc("price")) else matchingDS.orderBy(asc("price"))
      if (!matchingDS.isEmpty) {
        val matchedOrder = matchingDS.first()
        println(matchedOrder)
        val output = Match(order.orderId, matchedOrder.orderId, order.orderTime, quantity, matchedOrder.price)
        matchedDS = matchedDS.union(Seq(output).toDS())
        orderBookDS = orderBookDS.filter($"orderId" =!= matchedOrder.orderId)
      } else {
        orderBookDS = orderBookDS.union(Seq(order).toDS())
      }
      //orderBookDS.show()
    })
    matchedDS.show()
    matchedDS.repartition(1).write.option("header", value = true)
      .mode(SaveMode.Overwrite)
      .csv("coding_exercise/output.csv")
  }
}
