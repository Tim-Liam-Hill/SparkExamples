package com.mypackage

import org.apache.spark.sql.SparkSession
import org.apache.spark._ 
import org.apache.log4j._ 
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object Main{

    def main(args: Array[String]){

        val spark = SparkSession.builder.appName("Variable Delimiter").getOrCreate()

        import spark.implicits._
        val dept = Seq(
            ("50000.0#0#0#", "#"),
            ("0@1000.0@", "@"),
            ("1$", "$"),
            ("1000.00^Test_string", "^")).toDF("VALUES", "Delimiter")
        
        val ans = dept.withColumn("split_values", expr("split(VALUES,concat('\\\\', Delimiter))")) //needs 4 back slashes: first pair to escap into expr and second to escape in sql statement
        
        val extra = ans.withColumn("extra", array_except($"split_values",lit(Array(""))))
        extra.show(false)
    }
}
