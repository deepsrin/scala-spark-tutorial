package com.sparkTutorial.pairRdd.sort

import org.apache.spark.{SparkContext, SparkConf}

object MySortedWordCountSolution {

  def main(args : Array[String]): Unit = {
    val conf = new SparkConf().setAppName("sortedWordCount").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val wordsRDD = sc.textFile("in/word_count.text")

    val wordsList = wordsRDD.flatMap(line => line.split(" "))
    val wordToCount = wordsList.map(word => (word, 1))

    val wordToAggCount = wordToCount.reduceByKey((x,y) => x+y)
    val sortedWordToAggCount = wordToAggCount.sortBy(_._2, false)

    for ((word, count) <- sortedWordToAggCount.collect()) println(word + " = " + count)

  }

}
