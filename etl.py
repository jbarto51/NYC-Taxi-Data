from pyspark import SparkConf, SparkContext
import pyspark.pandas as ps
import datetime
import glob
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, month
#load file
conf = SparkConf()
conf.setAppName("etl")
conf.set("spark.executor.memory", "2g")
conf.set("spark.executor.cores", "1")
conf.set("spark.driver.memory", "8g")
conf.set("spark.driver.cores", "5")
spark = SparkContext(conf=conf)
for f in glob.glob('/data/nyctaxi/set1/*.parquet'):
    print(f)
    df = ps.read_parquet(f)
    print(df.columns)
    print(df.info(verbose=True))

#select columns
df = df[['VendorID', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance', 'PULocationID', 'DOLocationID', 'payment_type', 'fare_amount','mta_tax', 'tip_amount', 'tolls_amount', 'total_amount']]

#transform data by dropping null values
df = df.dropna()
# create month column
df = df.withColumn("month", month(df.tpep_pickup_datetime))
# save file to home directory
df.write.parquet("/home/nyctaxi.parquet")



