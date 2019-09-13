package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
object MySumOfNumbers {
    def main(args : Array[String]): Unit = {
      Logger.getLogger("org").setLevel(Level.ERROR)

      val conf = new SparkConf().setAppName("sumOfNumbers").setMaster("local[*]")
      val sc = new SparkContext(conf)

      val primeNumRdd = sc.textFile("in/prime_nums.text")

      val primeNums = primeNumRdd.flatMap(primeNum => primeNum.split("\\s+"))

      val validPrimeStrs = primeNums.filter(numStr => !numStr.isEmpty)
      val primeNumInts = validPrimeStrs.map(numStr => numStr.toInt)

      //for (primeNum <- primeNums) print("line = " + primeNum)

      val primeNums100 = primeNumInts.take(100)

      val sum = primeNums100.reduce((x, y) => x + y)

      print("sum = " + sum)
    }
}
