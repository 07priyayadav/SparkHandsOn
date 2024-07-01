#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 28 17:03:09 2024

@author: turtle
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.functions import col



spark = SparkSession.builder.appName("AvgFriendsByAgeUsingSqlDF").getOrCreate()

people = spark.read.option("header", "false").option("inferScehma", "true").csv("file:/Users/turtle/Documents/SparkCourse/fakefriends.csv")
people = people.toDF("ID", "name", "age", "friends")

people = people.withColumn("friends", col("friends").cast("int"))


print("Average number of friends by Age")   
people.groupBy("age").avg("friends").withColumnRenamed("avg(friends)", "avg_friends").show()

people.groupBy("age").avg("friends").sort("age").show()


people.groupBy("age").agg(func.round(func.avg("friends"),2).alias("friends_avg")).sort("age").show()

spark.stop()