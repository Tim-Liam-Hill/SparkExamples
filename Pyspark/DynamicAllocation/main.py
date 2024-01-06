from pyspark.sql import SparkSession 
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number, col
import sys

if __name__=="__main__":

    if( len(sys.argv) != 2):
        print("Usage: main.py <file_path>", file=sys.stderr)
        sys.exit(-1)
    
    spark = (SparkSession
             .builder
             .appName("Dynamic Allocation Experiment")
             .getOrCreate())


    #We could simply use the 'print' function for logging
    #but that isn't a good habit to develop.
    #As such, using the log4j facility spark uses. 
    log4jLogger = spark.sparkContext._jvm.org.apache.log4j 
    LOGGER = log4jLogger.LogManager.getLogger(__name__)
    LOGGER.info("Created logger")

    file_path = sys.argv[1]

    mnm_df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file_path))
    
    #challenge
    
    LOGGER.info("Getting the states with greatest count of each color")

    #Determine which states have the highest total count for each respective mnm_color (resulting df should have 1 entry for each color)

    mnm_top_states = mnm_df.groupBy("State","Color").sum("Count").withColumnRenamed("sum(Count)", "Total")
    win = Window.partitionBy("Color").orderBy(mnm_top_states.Total.desc())
    mnm_top_states.withColumn("row_number", row_number().over(win)) \
        .filter(col("row_number") == "1") \
        .select("Color","State", "Total").orderBy("Color").show()


    #Determine for each state which mnm_color has the greatest quantity.




