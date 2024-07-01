#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Jun 30 14:25:27 2024

@author: turtle
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField,StructType,IntegerType,StringType

spark = SparkSession.builder.appName("MostObscureSuperHero").getOrCreate()

schema = StructType([StructField("id",IntegerType(),True),StructField("name",StringType(),True)])

Names = spark.read.schema(schema).option("sep", " ").csv("file:/Users/turtle/Documents/SparkCourse/Marvel_Names.txt")

lines = spark.read.text("file:/Users/turtle/Documents/SparkCourse/Marvel_Graph.txt")

connections = lines.withColumn("id", func.split(func.col("value")," ")[0]).withColumn("connections", func.size(func.split(func.col("value")," "))-1).groupBy("id").agg(func.sum("connections").alias("connections"))



minconnection = connections.agg(func.min(func.col("connections"))).first()[0]

print("minconnection")
print(minconnection)

obscureconnections = connections.filter(func.col("connections")==minconnection)

obscureSuperHeros = obscureconnections.join(Names, "id").select("name","connections")


print("Superheroes with only single connection -")
obscureSuperHeros.show()

spark.stop()