package com.sparkTutorial.rdd.airports

import org.apache.spark.{SparkContext, SparkConf}
import com.sparkTutorial.commons.Utils


object MyAirportsByLatitudeSolution {

  def main(args : Array[String]) {

    val conf = new SparkConf().setAppName("airportsByLatitude").setMaster("local[2]");
    val sc = new SparkContext(conf);

    val airports = sc.textFile("in/airports.text");
    val airportsWithLat = airports.filter(line => line.split(Utils.COMMA_DELIMITER)(6).toDouble > 40)

    val airportNamesWithLat = airportsWithLat.map(line => {
      val splits = line.split(Utils.COMMA_DELIMITER)
      splits(1) + ", " + splits(6)
    })

    airportNamesWithLat.saveAsTextFile("out/airports_by_latitude2.text")

  }
}
