#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 28 16:36:53 2024

@author: turtle
"""

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("SparkSQLDataframe").getOrCreate()

people = spark.read.option("header", "true").option("inferSchema", "true").csv("file:/Users/turtle/Documents/SparkCourse/fakefriends-header.csv")

print("Inferred Schema/n")
people.printSchema()

print("People Name")
people.select("name").show()

print("Filter out anyone over 21")
people.filter(people.age < 21).show()

print("Group by age")
people.groupBy("age").count().show()

print("People with their Age Bracket")
people.select(people.name, (people.age%10) * 10).show()

spark.stop()