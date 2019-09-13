package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.spark.{SparkContext, SparkConf}

object MySameHostsSolution {

  def main(args : Array[String]) {
    val conf = new SparkConf().setAppName("nasalogs").setMaster("local[*]");
    val sc = new SparkContext(conf)

    val julyLogs = sc.textFile("in/nasa_19950701.tsv")
    val augLogs = sc.textFile("in/nasa_19950801.tsv")

    val julyHosts = julyLogs.map(line => line.split("\t")(0))
    val augHosts = augLogs.map(line => line.split("\t")(0))


    val intersection = julyHosts.intersection(augHosts)
    print("intersect = " + intersection.count())

    val cleanLogsIntersect = intersection.filter(host => host != "host")
    cleanLogsIntersect.saveAsTextFile("out/nasa_logs_same_hosts.csv")

  }

}
