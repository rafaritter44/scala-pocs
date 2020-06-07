package com.github.rafaritter44.scala_pocs.spark.core

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object TotalSpentByCustomer {
  
  def parseLine(line: String) = {
      val fields = line.split(",")
      val customerId = fields(0).toInt
      val amountSpent = fields(2).toFloat
      (customerId, amountSpent)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    new SparkContext("local[*]", "TotalSpentByCustomer")
        .textFile("./customer-orders.csv")
        .map(parseLine)
        .reduceByKey(_ + _)
        .map(_.swap)
        .sortByKey()
        .map(_.swap)
        .collect()
        .foreach(println)
  }
    
}
  