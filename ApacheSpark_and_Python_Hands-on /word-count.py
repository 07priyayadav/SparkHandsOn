#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 19:14:14 2024

@author: turtle
"""
from pyspark import SparkConf,SparkContext

import collections



conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

rdd = sc.textFile("file:/Users/turtle/Documents/SparkCourse/Book.txt")

wordRdd = rdd.flatMap(lambda x: x.split()).map(lambda x: (x,1))

wordCounts = wordRdd.reduceByKey(lambda x,y : x+y)

result = wordCounts.collect()

print('My logic')
for res in result:
    print(res)
    
words = rdd.flatMap(lambda x: x.split())
wordCount = words.countByValue()

print('By Course')
for word, count in wordCount.items():
    cleanWord = word.encode('ascii','ignore')
    
    if cleanWord:
        print(cleanWord, count)