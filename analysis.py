from pyspark import SparkConf, SparkContext
import pyspark.pandas as ps
import datetime
import glob
import pyarrow.parquet as pq
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, month
import matplotlib.pyplot as plt
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
#drop rows with missing values
df = df.dropna()
# create month column
df = df.withColumn("month", month(df.tpep_pickup_datetime))
#set date column
print(df['tpep_pickup_datetime'])
df['date'] = df['tpep_pickup_datetime'].dt.date
#print columns
print(df.columns)
#create hour and day of week columns
df['hour_of_day'] = df['tpep_pickup_datetime'].dt.hour
df['day_of_week'] = df['tpep_pickup_datetime'].dt.dayofweek

#average fare amount per number of passengers
average_fare_per_passenger = df.groupby('passenger_count')['fare_amount'].mean()
print("Average fare amount per number of passengers:", average_fare_per_passenger)

#average congestion surcharge for each hour of the day and each day of the week
average_congestion_surcharge = df.groupby(['hour_of_day', 'day_of_week'])['congestion_surcharge'].mean()
print("Average congestion surcharge for each hour of the day and each day of the week:", average_congestion_surcharge)

#most frequently occuring pickup location during night hours on weekends
night_hours = list(range(20, 24)) + list(range(0,6))
weekend_days = [5, 6]
filtered_df = df[(df['hour_of_day'].isin(night_hours)) & (df['day_of_week'].isin(weekend_days))]
pickup_count= filtered_df['PULocationID'].value_counts()
frequent_pickup_night = pickup_count.idxmax()
print("Most frequently occuring pickup location during night hours on weekends", frequent_pickup_night)
print("Frequency: ", pickup_count[frequent_pickup_night])


#Average fare amount for trips from the airport vs non-airport trips
df['airport_trips']= df['Airport_fee'] > 0
airport_fare = df.query('airport_trips')["fare_amount"].mean()
non_airport_fare = df.query('~airport_trips')["fare_amount"].mean()
plt.bar(['Airport', 'non-airport'], [airport_fare, non_airport_fare])
plt.title('Average fare amount for trips from the airport vs non-airport trips')
plt.xlabel('Location')
plt.ylabel('Fare amount')
plt.tight_layout()
plt.savefig('avg_fare_amount_airport_vs_non_airport.png')
plt.clf()

#Average congestion surcharge for each hour of the day and for each day of the week (grid)

xy = df.groupby(['day_of_week', 'hour_of_day'])['congestion_surcharge'].mean()
print(xy)
xy = xy.unstack()
print(xy)
plt.imshow(xy, cmap='viridis')
plt.colorbar()
plt.xticks(range(24), range(24))
plt.yticks(range(7), ['Mon', 'Tue', 'Wed', 'Thu', 'Fri', 'Sat', 'Sun'])
plt.xlabel('Hour of day')
plt.ylabel('Day of week')
plt.title('Congestion surcharge')
plt.tight_layout()
plt.savefig('avg_congestion_surcharge.png')
plt.clf()
