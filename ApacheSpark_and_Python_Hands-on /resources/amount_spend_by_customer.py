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

customerData = lines.map(parseLine).reduceByKey(lambda x,y: x+y)

#customerData = parsedLines.map(lambda x:(x[0],x[1])).reduceByKey(lambda x,y: x+y)

result = customerData.collect()

for res in result:
    customer = str(res[0])
    amount = res[1]
    print(f"CustomerID {customer} have spent amount {amount:.2f}")



