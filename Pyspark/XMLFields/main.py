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

    file_path = sys.argv[1]

    df = (spark.read.format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(file_path))
    
    df.show()
    df.select(df.ID, xpath(df.DOC, lit('root/name/first/text()'))).show()
