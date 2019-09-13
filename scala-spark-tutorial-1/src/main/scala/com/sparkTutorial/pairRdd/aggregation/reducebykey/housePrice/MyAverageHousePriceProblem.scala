package com.sparkTutorial.pairRdd.aggregation.reducebykey.housePrice

import org.apache.spark.{SparkContext, SparkConf}
import com.sparkTutorial.commons.Utils

import org.apache.spark.SparkConf
object MyAverageHousePriceProblem {
  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("averagehouseprice").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val houses = sc.textFile("in/RealEstate.csv")

    val cleanHouseRecords = houses.filter(line => !line.contains("Bedrooms"))

    val roomsToCountSalePrice = cleanHouseRecords.map(line => (line.split(Utils.COMMA_DELIMITER)(3).toInt, (1, line.split
    (Utils
      .COMMA_DELIMITER)(2).toDouble)))

    val roomsToCountAndPriceAgg = roomsToCountSalePrice.reduceByKey((x,y) => (x._1 + y._1, x._2 + y._2))

    val roomsToAvgPrice = roomsToCountAndPriceAgg.mapValues(x => x._2/x._1)
    val sortedHousePriceAvg = roomsToAvgPrice.sortByKey(false)

    for ((numRooms, avgPrice) <- sortedHousePriceAvg.collect()) println(numRooms + " = " + avgPrice)


  }
}
