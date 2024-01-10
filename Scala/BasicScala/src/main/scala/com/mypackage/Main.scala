package com.mypackage

import org.apache.spark.sql.SparkSession
import org.apache.spark._ 
import org.apache.log4j._ 

object Main{

    def main(args: Array[String]){
        val spark = SparkSession.builder.appName("Simple Application").getOrCreate()

        val myRange = spark.range(1000).toDF("number")
        myRange.show()
    }
}
