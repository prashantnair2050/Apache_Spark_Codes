package com.prashant.spark.dataloading

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object JSONExample {
  def main(args: Array[String]): Unit = {
    
    //Log Supress another way
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    //Create SparkSession
		
		val spark = SparkSession.builder
				.appName("JSON File Example")
				.master("local")
				.config("spark.sql.warehouse.dir","tmp/sparksql")  //Staging Dir is to maintain intermediate outputs
				.getOrCreate();
    
    //Loading a csv without headers
    
    val dataJSON = spark.read
                    .json("data/customerData.json")
                    
    dataJSON.printSchema()
    dataJSON.show(3)
    
    //Writing in JSON
    
    //Loading a tab - delimited file
    
    val dataTSV = spark.read
                    .option("header",true)
                    .option("inferSchema",true)
                    .option("delimiter", "\t")
                    .csv("data/file1")
                    
    dataTSV.write.option("inferSchema",true)
                 .json("output/output3")                
    
  }
}