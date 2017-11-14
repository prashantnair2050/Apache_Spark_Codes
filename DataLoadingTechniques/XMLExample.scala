package com.prashant.spark.dataloading

import org.apache.log4j.Logger
import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import com.databricks.spark.xml._
import org.apache.spark.sql.SQLContext

object XMLExample {
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
    
    //Load XML file
    //Challenge: Spark doesnot have default support to XML
    //Thus for this use-case, we need to add an external JAR provided by 
    //Databricks
    //Link: https://mvnrepository.com/artifact/com.databricks/spark-xml_2.11/0.4.1
    
    val XMLdata = spark.read
                       .format("com.databricks.spark.xml")
                       .option("rowTag", "employee")
                       .load("data/employee.xml")
                       
    XMLdata.show()
    XMLdata.printSchema()
     
    
    
    //Loading a tab - delimited file
    
    val dataTSV = spark.read
                    .option("header",true)
                    .option("inferSchema",true)
                    .option("delimiter", "\t")
                    .csv("data/file1")
                    
    dataTSV.write.format("com.databricks.spark.xml")
                 .option("inferSchema",true)
                 .option("rootTag", "hr")
                 .option("rowTag","emp")
                 .save("output/output4")
               
   println("XML file written successfully")
    
    
    
  }
}