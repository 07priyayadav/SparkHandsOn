#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 28 16:15:19 2024

@author: turtle
"""

from pyspark.sql import SparkSession, Row

spark = SparkSession.Builder().appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    
    return Row(ID=int(fields[0]), name=str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("file:/Users/turtle/Documents/SparkCourse/fakefriends.csv")
people = lines.map(mapper)

schemaPeople = spark.createDataFrame(people).cache()

schemaPeople.createOrReplaceTempView("people")

teenager = spark.sql("select * from people where age >= 13 and age <= 19")

for teen in teenager.collect():
    print(teen)
    
    
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()