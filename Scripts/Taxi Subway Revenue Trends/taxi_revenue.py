from __future__ import print_function

import sys
import math
import csv
import urllib2
from pyspark import SparkContext
from datetime import datetime
import time

def getTaxiCols(line):
    line = line.strip().split(',')
    key = line[0]
    fare = line[10]
    return (key,fare)

def calculateAverage(line):
    faresum = float(line[1][0])
    count = int(line[1][1])
    avg = faresum/count
    value = str(line[1][0])+','+str(line[1][1])+','+str(avg)
    return (line[0],value)

# Create SparkContext
sc = SparkContext(appName="PythonCrossProduct")

taxiData = sc.textFile(sys.argv[1], 1, use_unicode=False)

taxirddsumCount = taxiData.map(getTaxiCols).combineByKey(lambda value: (float(value), 1),
                                                 lambda x, value: (x[0] + float(value), x[1] + 1),
                                                 lambda x, y: (x[0] + y[0], x[1] + y[1]))

taxirddaverage = taxirddsumCount.map(calculateAverage).sortByKey()

output = taxirddaverage.collect()

for line in output:
    print(line[0]+','+str(line[1]))

sc.stop()
