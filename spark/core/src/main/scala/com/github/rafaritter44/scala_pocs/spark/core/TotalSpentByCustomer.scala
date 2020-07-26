package com.github.rafaritter44.scala_pocs.spark.core

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.log4j._

object TotalSpentByCustomer {
  
  val TextFile = 0
  
  def parseLine(line: String) = {
      val fields = line.split(",")
      val customerId = fields(0).toInt
      val amountSpent = fields(2).toFloat
      (customerId, amountSpent)
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
    conf.setAppName("TotalSpentByCustomer")
    new SparkContext(conf)
        .textFile(args.lift(TextFile).getOrElse("customer-orders.csv"))
        .map(parseLine)
        .reduceByKey(_ + _)
        .sortBy(_._2, false)
        .collect()
        .foreach(println)
  }
    
}
  