# Overview:
The project FxEngine contains the spark scala solution for a Trading Engine assessment problem by TCS.

## Build:
The project uses maven for dependency management. The main dependencies are:
- scala 2.13.0
- spark-core_2.13
- spark-sql_2.13

Jars created:
- Jar: FxEngine-1.0.0-SNAPSHOT.jar
- UberJar: FxEngine-1.0.0-SNAPSHOT-jar-with-dependencies.jar

Command To Run Application Locally with uber jar (assuming your are inside repo):
```
spark-submit --name "TradeEngine" --master local --class com.excercise.code.TradeExecution target/FxEngine-1.0.0-SNAPSHOT-jar-with-dependencies.jar input/inputOrders.csv
```
where `input/inputOrders.csv` is path to the input orders file.


## Code walkthrough:
1. The main class is TradeEngine.scala
2. There are two case classes defined i.e. `Order.scala` and `Match.scala` which represents an order and a match respectively. Since the schema is well defined we will be using Dataset[Order] and Dataset[Match] instead of a DataFrame in our program
3. Import the required libraries and packages in the main class
4. Create a Spark session on local and configure it
5. Define the schema for reading the input file by Encoding the `Order.scala` case class
6. Create the dataframe which will read the csv file and load the data into it. Convert the dataframe to `Dataset[Order]` and store in variable `inputOrdersDF`
7. Create datasets for empty orderbook `Dataset[order]` and empty matchedOrders `Dataset[Match]`
8. Applying the loop on `inputOrdersDF` dataset and do follwing for each input order:
    - Evaluate `type` of matching order and `quanity`
    - Filter matching orders from orderbook using the type and quantity evaluated above and store in `matchingOrders`.
    - Find best match out of the matching orders as following:
        - if orginal order is SELL then look for **Highest** BUY, so sort matching orders by price **descensing**
        - if orginal order is BUY then look for **Lowest** SELL, so sort matching orders by price **ascending**
    - If `matchingOrders` is empty then no match found. Simply place the input order in orderBook.
    - If `matchingOrders` is not empty then take first and create a `Match`, place it in matched orders DataSet[Match] and remove the  
      matched order from orderBook.
9. Write out matches orders in `output/matchedOrders.csv` and open/unmatched orders in `output/OpenOrders.csv`


### Input/Output:
The program takes 1 mandatory argument "ordersFilePath" which is the input file path containing all orders in below format.

```
orderId,user,orderTime,orderType,quantity,price
1,Steve,1623239770,SELL,72,1550
2,Ben,1623239771,BUY,8,148
3,Steve,1623239772,SELL,24,6612
4,Kim,1623239773,SELL,98,435
5,Sarah,1623239774,BUY,72,5039
6,Ben,1623239775,SELL,75,6356
7,Kim,1623239776,BUY,38,7957
8,Alex,1623239777,BUY,51,218
9,Jennifer,1623239778,SELL,29,204
10,Alicia,1623239779,BUY,89,7596
11,Alex,1623239780,BUY,70,2351
12,James,1623239781,SELL,89,4352
13,Sarah,1623239782,SELL,98,8302
14,Alicia,1623239783,BUY,56,8771
15,Alex,1623239784,BUY,83,737
16,Andrew,1623239785,SELL,15,61
17,Steve,1623239786,BUY,62,4381
18,Ben,1623239787,BUY,33,5843
19,Alicia,1623239788,BUY,20,5255
20,James,1623239789,SELL,68,4260
```

The program outputs 2 csv files:
- MatchedOrders: This file contains all the matches:
```
orderId1,orderId2,matchTime,quantity,price
5,1,1623239774,72,1550
12,10,1623239781,89,7596
```
- OpenOrders: This file contains open/unmacthed orders:
```
orderId,user,orderTime,orderType,quantity,price
2,Ben,1623239771,BUY,8,148
3,Steve,1623239772,SELL,24,6612
4,Kim,1623239773,SELL,98,435
6,Ben,1623239775,SELL,75,6356
7,Kim,1623239776,BUY,38,7957
8,Alex,1623239777,BUY,51,218
9,Jennifer,1623239778,SELL,29,204
11,Alex,1623239780,BUY,70,2351
13,Sarah,1623239782,SELL,98,8302
14,Alicia,1623239783,BUY,56,8771
15,Alex,1623239784,BUY,83,737
16,Andrew,1623239785,SELL,15,61
17,Steve,1623239786,BUY,62,4381
18,Ben,1623239787,BUY,33,5843
19,Alicia,1623239788,BUY,20,5255
20,James,1623239789,SELL,68,4260
```
      
