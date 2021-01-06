import time

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, IntegerType, DoubleType, TimestampType

import datetime


def timestamp_diff(time1: datetime.datetime, time2: datetime.datetime):
    return int((time1 - time2).total_seconds())


spark = SparkSession \
    .builder \
    .appName('pySpark sample') \
    .getOrCreate()

spark.udf.register("timestamp_diff", timestamp_diff)

# get data from Taxi Ride data dataset
print('load data from "Taxi Ride" data dataset')
start_time = time.time()

taxiRide_schema = StructType() \
    .add("rideId", IntegerType(), True) \
    .add("isStart", StringType(), True) \
    .add("endTime", TimestampType(), True) \
    .add("startTime", TimestampType(), True) \
    .add("startLon", DoubleType(), True) \
    .add("startLat", DoubleType(), True) \
    .add("endLon", DoubleType(), True) \
    .add("endLat", DoubleType(), True) \
    .add("passengerCnt", IntegerType(), True)

df_taxiRide_data_clean = spark.read.format("csv") \
    .option("header", False) \
    .schema(taxiRide_schema) \
    .load("data/nycTaxiRides_all")

df_taxiRide_data_clean.printSchema()

df_taxiRide_data_clean.show(n=5, truncate=False)
print(df_taxiRide_data_clean.count())

print("--- %s 'Taxi Ride data cleansing' in seconds ---" % (time.time() - start_time))

# get data from Taxi Fares data dataset
print('load data from "Taxi Fare" data dataset')
start_time = time.time()

taxiFare_schema = StructType() \
    .add("rideId", IntegerType(), True) \
    .add("taxiId", IntegerType(), True) \
    .add("driverId", IntegerType(), True) \
    .add("transactionTime", TimestampType(), True) \
    .add("paymentType", StringType(), True) \
    .add("tip", DoubleType(), True) \
    .add("tolls", DoubleType(), True) \
    .add("totalFare", DoubleType(), True)

df_taxiFare_data_clean = spark.read.format("csv") \
    .option("header", False) \
    .schema(taxiFare_schema) \
    .load("data/nycTaxiFares_all")

df_taxiFare_data_clean.printSchema()

df_taxiFare_data_clean.show(n=5, truncate=False)
print(df_taxiFare_data_clean.count())

print("--- %s 'Taxi Fare data cleansing' in seconds ---" % (time.time() - start_time))

print("test sql's")

df_taxiRide_data_clean.createOrReplaceTempView("view_taxirides")
df_taxiFare_data_clean.createOrReplaceTempView("view_taxifares")

spark.sql("SELECT * FROM view_taxirides WHERE rideId = 914155").show()
spark.sql("SELECT * FROM view_taxifares WHERE rideId = 914155").show()

# join
start_time = time.time()

spark.sql("SELECT view_taxirides.rideId, startTime, endTime, timestamp_diff(endTime, startTime) as timeDiff"
          ", startLat, startLon, endLat, endLon"
          ", passengerCnt, taxiId, driverId, transactionTime, paymentType, tip, tolls, totalFare"
          " FROM view_taxirides, view_taxifares"
          " WHERE view_taxirides.rideId = view_taxifares.rideId"
          " AND isStart = 'END'"
          " ORDER BY totalFare DESC") \
    .show()
print("--- %s ' Join clean Taxi Rides and Fares' in seconds ---" % (time.time() - start_time))
