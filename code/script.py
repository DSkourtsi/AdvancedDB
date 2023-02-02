from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import DataFrame
from functools import reduce
import time, csv
import os


spark = SparkSession.builder.master("spark://192.168.0.2:7077").appName("Project").getOrCreate()
print("spark session created")

#Create tripdata dfs
jan = spark.read.parquet("hdfs://master:9000/user/user/project/yellow_tripdata_2022-01.parquet")
feb = spark.read.parquet("hdfs://master:9000/user/user/project/yellow_tripdata_2022-02.parquet")
mar = spark.read.parquet("hdfs://master:9000/user/user/project/yellow_tripdata_2022-03.parquet")
apr = spark.read.parquet("hdfs://master:9000/user/user/project/yellow_tripdata_2022-04.parquet")
may = spark.read.parquet("hdfs://master:9000/user/user/project/yellow_tripdata_2022-05.parquet")
jun = spark.read.parquet("hdfs://master:9000/user/user/project/yellow_tripdata_2022-06.parquet")
dfs = [jan,feb,mar,apr,may,jun]
tripdata = reduce(DataFrame.unionAll, dfs)
#notices some false data so we cleaned the dataframe 
tripdata = tripdata.where(tripdata.tpep_pickup_datetime >= "2022-01-01").where(tripdata.tpep_pickup_datetime < "2022-07-01")
tripdata.printSchema()
tripdata.show()
tripdata_rdd = tripdata.rdd

#create location lookup dfs
locations = spark.read.load("hdfs://master:9000/user/user/project/taxi+_zone_lookup.csv", format="csv", inferSchema="true", header="true")
locations.printSchema()
locations.show()
locations_rdd = locations.rdd

#create Temporary Views needed for SQL Queries
tripdata.createOrReplaceTempView("tripdata_view")
locations.createOrReplaceTempView("locations_view")

#SQL Queries
q1 = "SELECT * \
      FROM tripdata_view \
      WHERE DOLocationID = \
      (SELECT LocationID \
        FROM locations_view \
        WHERE Zone='Battery Park') \
      ORDER BY tip_amount DESC LIMIT 1"

q2 = "SELECT MONTH(tpep_pickup_datetime) as month, *\
      FROM tripdata_view\
      WHERE tolls_amount IN(\
        SELECT MAX(tolls_amount)\
        FROM tripdata_view\
        GROUP BY month(tpep_pickup_datetime)\
      ) \
      ORDER BY month ASC"

q3td = tripdata.where(tripdata.PULocationID != tripdata.DOLocationID)
q3 = q3td.groupBy(window("tpep_pickup_datetime", "15 days", startTime="2 days 22 hours"))\
.agg(avg("total_amount").alias("average amount"), avg("trip_distance").alias("average distance"))\
.sort("window").select('window.start', 'window.end', 'average amount', 'average distance')

q4 = "SELECT * \
      FROM (SELECT day, hour, average_passengers, rank() OVER (PARTITION BY day ORDER BY average_passengers DESC) as rank \
            FROM (SELECT DAYOFWEEK(tpep_pickup_datetime) as day, \
                         HOUR(tpep_pickup_datetime) as hour, \
                         avg(passenger_count) as average_passengers \
                  FROM tripdata_view \
                  GROUP BY day, hour)) \
      WHERE rank<4 \
      ORDER BY day, rank"

q5 = "SELECT * \
          FROM  (SELECT month, day, tip_percentange, rank() OVER (PARTITION BY MONTH(day) ORDER BY tip_percentange DESC) as rank \
                 FROM (SELECT MONTH(tpep_pickup_datetime) as month, DATE(tpep_pickup_datetime) as day, avg(tip_amount/fare_amount * 100) as tip_percentange\
                       FROM tripdata_view \
                       GROUP BY day)) \
          WHERE rank<6 \
          ORDER BY month, rank"

#Q1 SQL API
sum = 0
for i in range(10):
      result = spark.sql(q1)
      start = time.time()
      list = result.collect()
      end = time.time()
      sum += end-start #total time for query execution and write to hdfs
      q1_res = spark.createDataFrame(list)
q1_res.show()
q1_time = sum/10

#Q2 SQL API 
sum = 0
for i in range(10):
      result = spark.sql(q2)
      start = time.time()
      list = result.collect()
      end = time.time()
      sum += end-start #total time for query execution and write to hdfs
      q2_res = spark.createDataFrame(list)
q2_res.show()
q2_time = sum/10

#Q3 Dataframe
sum = 0
for i in range(10):
      start = time.time()
      list = q3.collect()
      end = time.time()
      sum += end-start #total time for query execution and write to hdfs
      q3_res = spark.createDataFrame(list)
q3_res.show()
q3_time = sum/10

#Q4 SQL API 
sum = 0
for i in range(10):
      result = spark.sql(q4)
      start = time.time()
      list = result.collect()
      end = time.time()
      sum += end-start #total time for query execution and write to hdfs
      q4_res = spark.createDataFrame(list)
q4_res.show()
q4_time = sum/10

#Q5 SQL API 
sum = 0
for i in range(10):
      result = spark.sql(q5)
      start = time.time()
      list = result.collect()
      end = time.time()
      sum = end-start #total time for query execution and write to hdfs
      q5_res = spark.createDataFrame(list)
q5_res.show()
q5_time = sum/10

q1_res.coalesce(1).write.options(header='True').mode('overwrite').csv('hdfs://master:9000/user/user/results/q1_res')
q2_res.coalesce(1).write.options(header='True').mode('overwrite').csv('hdfs://master:9000/user/user/results/q2_res')
q3_res.coalesce(1).write.options(header='True').mode('overwrite').csv('hdfs://master:9000/user/user/results/q3_res')
q4_res.coalesce(1).write.options(header='True').mode('overwrite').csv('hdfs://master:9000/user/user/results/q4_res')
q5_res.coalesce(1).write.options(header='True').mode('overwrite').csv('hdfs://master:9000/user/user/results/q5_res')