#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 28 21:33:23 2024

@author: turtle
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField,IntegerType,FloatType,StringType,StructType

spark = SparkSession.builder.appName("MinTemperatures").getOrCreate()

schema = StructType([StructField("stationID",StringType(),True), StructField("date",IntegerType(),True), StructField("measureType",StringType(),True), StructField("temperature",FloatType(),True)])
    
df = spark.read.schema(schema).csv("file:/Users/turtle/Documents/SparkCourse/1800.csv")

df.printSchema()

minTemps = df.filter(df.measureType == "TMIN")

stationTemps = minTemps.select("stationID","temperature")

minTemperatureStation = stationTemps.groupBy("stationID").agg(func.min("temperature").alias("temperature"))

minTemperatureStation.show()

minTemperatureStationF = minTemperatureStation.withColumn("temperatureF",func.round(func.col("temperature") * 0.1 * (9.0 / 5.0) + 32.0, 2)).select("stationID", "temperatureF").sort("temperatureF")

minTemperatureStationF.show()
                                                
# Collect, format, and print the results
results = minTemperatureStationF.collect()

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1])) 
    
    
    
spark.stop()