## Goal

To learn how to map written scala code to the DAG output in Spark UI 


## Application requirements 


* Read in the retail data https://github.com/databricks/Spark-The-Definitive-Guide/tree/master/data/retail-data/by-day
* in a single data frame have the total quantity for all dates and countries, then have counts for each date, then each date and country  -> write this out as parquet with snappy compression
* get 10 invoices with largest total cost -> write to single csv file
