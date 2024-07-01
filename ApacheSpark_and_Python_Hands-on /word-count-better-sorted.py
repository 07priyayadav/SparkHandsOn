#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 20:37:06 2024

@author: turtle
"""

from pyspark import SparkConf,SparkContext
import re

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())

rdd = sc.textFile("file:/Users/turtle/Documents/SparkCourse/Book.txt")

wordRdd = rdd.flatMap(normalizeWords)
wordCounts = wordRdd.map(lambda x: (x,1)).reduceByKey(lambda x,y : x+y)
wordCountsSorted = wordCounts.map(lambda x: (x[1], x[0])).sortByKey()

result = wordCountsSorted.collect()


print('By Course')

for res in result:
    count = str(res[0])
    word = res[1].encode('ascii', 'ignore')
    if (word):
        print(word.decode() + ":\t" + count)