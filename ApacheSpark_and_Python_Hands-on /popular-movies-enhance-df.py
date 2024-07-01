#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 29 20:24:52 2024

@author: turtle
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,IntegerType,LongType
import codecs


def loadMovieNames():
    movieNames = {}
    
    with codecs.open("/Users/turtle/Documents/SparkCourse/ml-100k/u.item", "r", encoding='ISO-8859-1', errors='ignore') as f:
        for line in f:
            fields = line.split('|')
            movieNames[int(fields[0])] = fields[1]
        return movieNames


spark = SparkSession.builder.appName("PopularMovieNames").getOrCreate()

nameDict = spark.sparkContext.broadcast(loadMovieNames())

schema = StructType([StructField("userID", IntegerType(),True),StructField("movieId", IntegerType(),True),StructField("rating", IntegerType(),True),StructField("timestamp", LongType(),True)])

moviesdf = spark.read.option("sep", "\t").schema(schema).csv("file:/Users/turtle/Documents/SparkCourse/ml-100k/u.data")

movieCount = moviesdf.groupBy("movieId").count()

def lookupName(movieId):
    return nameDict.value[movieId]


lookupNameUDF = func.udf(lookupName)

moviesWithNames = movieCount.withColumn("movieName",lookupNameUDF(func.col("movieId")))

sortedMovieNames = moviesWithNames.orderBy(func.desc("count"))

sortedMovieNames.show()

spark.stop()

