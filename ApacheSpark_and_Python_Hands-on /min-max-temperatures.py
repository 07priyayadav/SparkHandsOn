#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 17:49:37 2024

@author: turtle
"""

from pyspark import SparkConf, SparkContext

import collections

conf = SparkConf().setMaster("local").setAppName("MinimumTemperatures")
sc = SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    stationId = fields[0]
    tmeasure = fields[2] 
    temp = float(fields[3]) * 0.1 * ( 9.0/5.0) + 32.0
    
    return (stationId,tmeasure,temp)

lines = sc.textFile("file:/Users/turtle/Documents/SparkCourse/1800.csv")
parsedLines = lines.map(parseLine)

minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationMinTemps = minTemps.map(lambda x:(x[0],x[2]))
minTemps = stationMinTemps.reduceByKey(lambda x,y: min(x,y))

result = minTemps.collect()

print(" Min temperature by Weather Station")
for res in result:
    print(res[0] + "\t{:.2f}F".format(res[1]))
    
maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationMaxTemps = maxTemps.map(lambda x:(x[0],x[2]))
maxTemps = stationMaxTemps.reduceByKey(lambda x,y: max(x,y))

result = maxTemps.collect()

print(" Max temperature by Weather Station")
for res in result:
    print(res[0] + "\t{:.2f}F".format(res[1]))
    
    
# Stop the Spark context
sc.stop()