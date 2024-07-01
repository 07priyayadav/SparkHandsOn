#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 30 13:41:45 2024

@author: turtle
"""

from pyspark.sql import SparkSession
from pyspark.sql.types import StructType,StructField,IntegerType,StringType
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("PopularSuperhero").getOrCreate()
schema = StructType([StructField("id", IntegerType(),True),StructField("name", StringType(),True)])

superheroName = spark.read.schema(schema).option("sep", " ").csv("file:/Users/turtle/Documents/SparkCourse/Marvel_Names.txt")

lines = spark.read.text("file:/Users/turtle/Documents/SparkCourse/Marvel_Graph.txt")

lines.show(5)

connections = lines.withColumn("id", func.split(func.col("value")," ")[0]).withColumn("connections", func.size(func.split(func.col("value")," "))-1).groupBy("id").agg(func.sum("connections").alias("connections"))

mostPopular = connections.sort(func.col("connections").desc()).first()

print(mostPopular)

mostPopularName = superheroName.filter(func.col("id")==mostPopular[0]).select("name").first()

print(mostPopularName)

print(mostPopularName[0] + "  is the most popular superhero with " + str(mostPopular[1]) + " connections")