package com.sundogsoftware.spark

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.sql._
import org.apache.log4j._

object SparkSQL {
  
  case class Person(id: Int, name: String, age: Int, numFriends: Int)
  
  def parseLine(line: String): Person = {
    val fields = line.split(',')  
    val person: Person = Person(fields(0).toInt, fields(1), fields(2).toInt, fields(3).toInt)
    return person
  }
  
  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession
      .builder
      .appName("SparkSQL")
      .master("local[*]")
      .getOrCreate()
    
    val lines = session.sparkContext.textFile("fakefriends.csv")
    val people = lines.map(parseLine)
    
    // Infer the schema, and register the DataSet as a table.
    import session.implicits._
    val peopleSchema = people.toDS
    
    peopleSchema.printSchema()
    
    peopleSchema.groupBy("age").count().orderBy($"count".desc).show()
    
    // Register the DataFrame as a temporary view.
    peopleSchema.createOrReplaceTempView("people")
    
    // SQL can be run over DataFrames that have been registered as tables.
    val teenagers = session.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19")
    
    val results = teenagers.collect()
    
    results.foreach(println)
    
    session.stop()
  }
}