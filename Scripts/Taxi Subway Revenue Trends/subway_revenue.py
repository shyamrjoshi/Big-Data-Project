from __future__ import print_function

import sys
import math
import csv
from pyspark import SparkContext
from datetime import datetime
import time

def getSubwayCols(line):
    cols = line.strip().split(',')
    turnstiledate = cols[6]
    turnstiletime = cols[7]
    date_object = datetime.strptime(turnstiledate, '%m/%d/%Y')
    turnstiledate = date_object.strftime('%Y%m%d')
    date_object = datetime.strptime(turnstiletime, '%H:%M:%S')
    turnstiletime = date_object.strftime('%H00')
    key = str(turnstiledate) + str(turnstiletime)
    value = (cols[9], cols[10])
    return (key,value)


# Create SparkContext
sc = SparkContext(appName="PythonCrossProduct")

subwayData = sc.textFile(sys.argv[1]+"/*.txt", 1, use_unicode=False)

subwayrdd = subwayData.map(getSubwayCols).combineByKey(lambda value: (int(value[0]), int(value[1]), 1),
                                                       lambda x, value: (x[0] + int(value[0]),x[0] + int(value[1]), x[2] + 1),
                                                       lambda x, y: (x[0] + y[0], x[1] + y[1], x[2] + y[2])).sortByKey()

output = subwayrdd.collect()

for line in output:
    print(line[0]+','+str(line[1][0])+','+str(line[1][1])+','+str(line[1][2]))

sc.stop()
