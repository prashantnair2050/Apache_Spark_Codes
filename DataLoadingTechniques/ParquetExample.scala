package com.prashant.spark.dataloading

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession

object ParquetExample {
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
    
    // Lets load a normal csv file and convert it into parquet data
    
    val dataCSV = spark.read
                    .option("inferSchema", true)
                    .csv("data/txns.csv")
                    
    val dataParquet = dataCSV.write.parquet("data/txns_parquet")
    
    //Lets load the parquet data
    
    val txnParquet = spark.read
                          .parquet("data/txns_parquet")
                          
                          
                          
    txnParquet.printSchema()
    txnParquet.show(4)
    
  }
}