package com.prashant.spark.dataloading

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object CompressedFileExample {
  def main(args: Array[String]): Unit = {
    
    //Log Supress another way
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    Logger.getLogger("akka").setLevel(Level.ERROR)
    
    //Create SparkSession
		
		val spark = SparkSession.builder
				.appName("XML File Example")
				.master("local")
				.config("spark.sql.warehouse.dir","tmp/sparksql")  //Staging Dir is to maintain intermediate outputs
				.getOrCreate();
    
    //Load a delimited file and compress the output using encoder
    
    //Loading a tab - delimited file
    
    val dataTSV = spark.read
                    .option("header",true)
                    .option("inferSchema",true)
                    .option("delimiter", "\t")
                    .csv("data/file1")
                    
    dataTSV.write
           .option("compression","gzip")
           .csv("output/output5") 
           
           
           println("GZIP done")
           
    dataTSV.write
           .option("compression","org.apache.hadoop.io.compress.BZip2Codec")
           .csv("output/output6")
           
           println("BZIP done")
   
           
     
           
  }
}