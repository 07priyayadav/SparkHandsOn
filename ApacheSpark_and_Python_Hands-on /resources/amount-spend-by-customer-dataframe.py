#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jun 29 13:11:16 2024

@author: turtle
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType,StructField,FloatType,StringType

spark = SparkSession.builder.appName("TotalSpendByCustomer").getOrCreate()

schema = StructType([ StructField("custID",StringType(),True), StructField("orderID",StringType(),True), StructField("amount",FloatType(),True)])

df = spark.read.schema(schema).csv("file:/Users/turtle/Documents/SparkCourse/customer-orders.csv")

df.printSchema()

customerSpend = df.select("custID","amount")

totCustomSpend = customerSpend.groupBy("custID").agg(func.round(func.sum("amount"),2).alias("total_spend"))

totCustomSpendSorted = totCustomSpend.sort("total_spend")

print("total Customer")

totCustomSpendSorted.show(totCustomSpendSorted.count())

'''result = totCustomSpendSorted.collect()

for res in result:
    
    print(res[0] + "\t${:2f}".format(res[1]))'''
    
spark.stop()