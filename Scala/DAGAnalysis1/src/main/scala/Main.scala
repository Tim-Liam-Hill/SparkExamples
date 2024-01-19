package com.mypackage

import org.apache.spark.sql.SparkSession
import org.apache.spark._ 
import org.apache.log4j._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Main{

    def main(args: Array[String]){

        if(args.length != 2){
            println("Usage: jar <input location> <output location>")
            sys.exit(1)
        }

        val spark = SparkSession.builder.appName("DAG Analysis").getOrCreate()

        val schema = StructType(Array(
            StructField("InvoiceNo",StringType,true),
            StructField("StockCode",StringType,true),
            StructField("Description",StringType,true),
            StructField("Quantity",IntegerType,true),
            StructField("InvoiceDate",TimestampType,true),
            StructField("UnitPrice",DoubleType,true),
            StructField("CustomerID",DoubleType,true),
            StructField("Country",StringType,true))
        )
        
        val df = spark.read.option("header","true").schema(schema).format("csv").load(args(0))
        println(df.where( col("Country") === "Spain" ).count())
        spark.stop()
    }
}
