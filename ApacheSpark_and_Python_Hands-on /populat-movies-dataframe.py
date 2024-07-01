#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 29 19:54:11 2024

@author: turtle
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructField,IntegerType,LongType,StructType

spark = SparkSession.builder.appName("PopularMovies").getOrCreate()

schema = StructType([StructField("userID", IntegerType(),True),StructField("movieId", IntegerType(),True),StructField("rating", IntegerType(),True),StructField("timestamp", LongType(),True)])

df = spark.read.option("sep", "\t").schema(schema).csv("file:/Users/turtle/Documents/SparkCourse/ml-100k/u.data")

moviesDF = df.select("movieId","rating")

topMovies = moviesDF.groupBy("movieId").count().orderBy(func.desc("count"))

top10Movies = topMovies.show(10)

spark.stop()