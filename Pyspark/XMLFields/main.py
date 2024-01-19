from pyspark.sql import SparkSession 
from pyspark.sql.functions import xpath, lit
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

    df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file_path))
    
    df.show()
    df.select(df.ID, xpath(df.DOC, lit('root/name/first/text()'))).show()
