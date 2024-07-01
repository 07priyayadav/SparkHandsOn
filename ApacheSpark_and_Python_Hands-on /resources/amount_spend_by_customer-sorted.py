#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jun 27 20:55:58 2024

@author: turtle
"""

from pyspark import SparkConf, SparkContext

def parseLine(line):
    fields = line.split(',')
    return (int(fields[0]),float(fields[2]))


conf = SparkConf().setMaster("local").setAppName("AmountSpendByCustomer")
sc = SparkContext(conf = conf)

lines = sc.textFile("file:/Users/turtle/Documents/SparkCourse/customer-orders.csv")

parsedLines = lines.map(parseLine)

customerSpend = parsedLines.reduceByKey(lambda x,y:x+y).sortBy(lambda x:x[1],ascending= False)

result = customerSpend.collect()

for res in result:
    customer = str(res[0])
    amount = res[1]
    print(f"{customer}\t{amount:.2f}")





