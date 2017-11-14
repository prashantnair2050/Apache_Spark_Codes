package com.prashant.spark.dataloading

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object CSVExample {
  def main(args: Array[String]): Unit = {
  
      
      //Log Supress another way
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    //Create SparkSession
		
		val spark = SparkSession.builder
				.appName("CSV File Example")
				.master("local")
				.config("spark.sql.warehouse.dir","tmp/sparksql")  //Staging Dir is to maintain intermediate outputs
				.getOrCreate();
    
    //Loading a csv without headers
    
    val dataCSV = spark.read
                    .option("inferSchema", true)
                    .csv("data/txns.csv")
                    
    //Display the contents
    dataCSV.show(2)
    dataCSV.printSchema()
    
    //Adding Headers manually in another Dataset
    // add import org.apache.spark.sql.functions._
    val dataWithHeaderCSV = dataCSV.select(col("_c0").as("txnid"), 
                                          col("_c1").as("date"),
                                          col("_c2").as("custid"),
                                          col("_c3").as("amount"),
                                          col("_c4").as("category"),
                                          col("_c5").as("subcategory"),
                                          col("_c6").as("city"),
                                          col("_c7").as("state"),
                                          col("_c8").as("txntype"))
                                          
     dataWithHeaderCSV.printSchema()
     dataWithHeaderCSV.show(2)
    
    //Limit the contents to 3 records
    val threeRecords = dataWithHeaderCSV.limit(3)
    
    //Save the output in the file with delimiter as | (pipe) character
    threeRecords.write
                .option("header", true)
                .csv("output/output2")
    
      
    
  }
}