#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 20:13:44 2024

@author: turtle
"""
from pyspark import SparkConf,SparkContext
import re
import collections

conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf = conf)

def normalizeWords(text):
    return re.compile(r'\W+',re.UNICODE).split(text.lower())

rdd = sc.textFile("file:/Users/turtle/Documents/SparkCourse/Book.txt")

words = rdd.flatMap(normalizeWords)
wordCount = words.countByValue()

print('By Course')
for word, count in wordCount.items():
    cleanWord = word.encode('ascii','ignore')
    
    if cleanWord:
        print(cleanWord, count)