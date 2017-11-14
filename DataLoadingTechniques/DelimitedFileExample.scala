package com.prashant.spark.dataloading

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object DelimitedFileExample {
  def main(args: Array[String]): Unit = {
    
    //Log Supress another way
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    //Create SparkSession
		
		val spark = SparkSession.builder
				.appName("Delimited File Example")
				.master("local")
				.config("spark.sql.warehouse.dir","tmp/sparksql")  //Staging Dir is to maintain intermediate outputs
				.getOrCreate();
    
    //Loading a tab - delimited file
    
    val dataTSV = spark.read
                    .option("header",true)
                    .option("delimiter", "\t")
                    .csv("data/file1")
    //Display the contents
    dataTSV.show()
    
    //Limit the contents to 3 records
    val threeRecords = dataTSV.limit(3)
    
    //Save the output in the file with delimiter as | (pipe) character
    threeRecords.write
                .option("header", true)
                .option("delimiter","|")
                .csv("output/output1")
  }
}