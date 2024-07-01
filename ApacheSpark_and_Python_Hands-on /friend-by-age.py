#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 16:39:05 2024

@author: turtle
"""

from pyspark import SparkConf,SparkContext

conf = SparkConf().setMaster("local").setAppName("FriendsByAge") 
sc = SparkContext(conf = conf)

def parseLine(line):
    
    fields = line.split(',')
    age = int(fields[2])
    friends = int(fields[3])
    return (age,friends)

def ageBracket(age):
    
    return (age//10) *10


lines = sc.textFile("file:/Users/turtle/Documents/SparkCourse/fakefriends.csv")

rdd = lines.map(parseLine)

bracketedRdd = rdd.map(lambda x:(ageBracket(x[0]),x[1]))
totalbyAgeBracket = bracketedRdd.mapValues(lambda x:(x,1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avgByAgeBracket = totalbyAgeBracket.mapValues(lambda x: x[0] // x[1])
result = avgByAgeBracket.collect()

# Print the results
for bracket, avgFriends in result:
    print(f"Age bracket {bracket}-{bracket+9} has average {avgFriends} friends")



totalbyAge = rdd.mapValues(lambda x: (x, 1)).reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))
avgByAge = totalbyAge.mapValues(lambda x: x[0] // x[1])
result = avgByAge.collect()

ageWithMaxFriends = avgByAge.max(lambda x:x[1])
print(f"Age with max average number of friends: {ageWithMaxFriends[0]} with average {ageWithMaxFriends[1]} friends")

ageWithMinFriends = avgByAge.min(lambda x:x[1])
print(f"Age with min average number of friends: {ageWithMinFriends[0]} with average {ageWithMinFriends[1]} friends")


for res in result:
    print(res)
    
# Stop the Spark context
sc.stop()