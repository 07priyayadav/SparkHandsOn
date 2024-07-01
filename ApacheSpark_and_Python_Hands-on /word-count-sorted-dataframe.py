#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Jun 28 21:06:04 2024

@author: turtle
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text("file:/Users/turtle/Documents/SparkCourse/book copy.txt")

words = inputDF.select(func.explode(func.split(inputDF.value,"\\W+")).alias("word"))

wordsWithoutEmptyString = words.filter(words.word != "")

lowerCaseWords = wordsWithoutEmptyString.select(func.lower(wordsWithoutEmptyString.word).alias("word"))

wordCounts = lowerCaseWords.groupBy("word").count()

wordCountsSorted = wordCounts.sort("count")


wordCountsSorted.show(wordCountsSorted.count())


spark.stop()


