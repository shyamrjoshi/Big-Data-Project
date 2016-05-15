from __future__ import print_function

import sys
import math
import csv
from pyspark import SparkContext
from datetime import datetime
import time

list = []
list.append(time.strptime("0000","%H%M"))
list.append(time.strptime("0051","%H%M"))
list.append(time.strptime("0151","%H%M"))
list.append(time.strptime("0251","%H%M"))
list.append(time.strptime("0300","%H%M"))
list.append(time.strptime("0351","%H%M"))
list.append(time.strptime("0451","%H%M"))
list.append(time.strptime("0459","%H%M"))
list.append(time.strptime("0600","%H%M"))
list.append(time.strptime("0651","%H%M"))
list.append(time.strptime("0751","%H%M"))
list.append(time.strptime("0851","%H%M"))
list.append(time.strptime("0900","%H%M"))
list.append(time.strptime("0951","%H%M"))
list.append(time.strptime("1051","%H%M"))
list.append(time.strptime("1151","%H%M"))
list.append(time.strptime("1200","%H%M"))
list.append(time.strptime("1251","%H%M"))
list.append(time.strptime("1351","%H%M"))
list.append(time.strptime("1451","%H%M"))
list.append(time.strptime("1500","%H%M"))
list.append(time.strptime("1551","%H%M"))
list.append(time.strptime("1651","%H%M"))
list.append(time.strptime("1751","%H%M"))
list.append(time.strptime("1800","%H%M"))
list.append(time.strptime("1851","%H%M"))
list.append(time.strptime("1951","%H%M"))
list.append(time.strptime("2051","%H%M"))
list.append(time.strptime("2100","%H%M"))
list.append(time.strptime("2151","%H%M"))
list.append(time.strptime("2251","%H%M"))
list.append(time.strptime("2351","%H%M"))

def getTimeInterval(x):
    mydatetime = datetime.strptime(x,'%Y%m%d%H%M')
    hr = mydatetime.hour
    min = mydatetime.minute
    for timeelement in list:
        if hr > timeelement[3]:
            interval = timeelement
        elif hr == timeelement[3]:
            if min >= timeelement[4]:
                interval = timeelement
        else:
            break

    mydatetime = mydatetime.replace(hour=int(interval[3]))
    mydatetime = mydatetime.replace(minute=int(interval[4]))
    newdate = mydatetime.strftime('%Y%m%d%H%M')
    return str(newdate)

def distanceBetweenLatLong(lat1, lon1, lat2, lon2):
    lat1 = float(lat1)
    lat2 = float(lat2)
    lon1 = float(lon1)
    lon2 = float(lon2)
    a = 6378137.0
    b = 6356752.314245
    f = 1 / 298.257223563

    L = math.radians(lon2 - lon1)

    U1 = math.atan((1 - f) * math.tan(math.radians(lat1)));
    U2 = math.atan((1 - f) * math.tan(math.radians(lat2)));
    sinU1 = math.sin(U1)
    cosU1 = math.cos(U1)
    sinU2 = math.sin(U2)
    cosU2 = math.cos(U2)
    cosSqAlpha = float()
    sinSigma = float()
    cos2SigmaM = float()
    cosSigma = float()
    sigma = float()

    # l == lambda
    l = L
    lambdaP = float()
    iterLimit = 100

    while True:

        sinLambda = math.sin(l)
        cosLambda = math.cos(l)
        sinSigma = math.sqrt((cosU2 * sinLambda) * (cosU2 * sinLambda) + (cosU1 * sinU2 - sinU1 * cosU2 * cosLambda) * (
        cosU1 * sinU2 - sinU1 * cosU2 * cosLambda))
        if (sinSigma == 0):
            return 0;

        cosSigma = sinU1 * sinU2 + cosU1 * cosU2 * cosLambda
        sigma = math.atan2(sinSigma, cosSigma)
        sinAlpha = cosU1 * cosU2 * sinLambda / sinSigma
        cosSqAlpha = 1 - sinAlpha * sinAlpha
        cos2SigmaM = cosSigma - 2 * sinU1 * sinU2 / cosSqAlpha

        C = f / 16 * cosSqAlpha * (4 + f * (4 - 3 * cosSqAlpha))
        lambdaP = l
        l = L + (1 - C) * f * sinAlpha * (
        sigma + C * sinSigma * (cos2SigmaM + C * cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM)));

        if (iterLimit == 0) or ((math.fabs(l - lambdaP) > 1e-12) and (iterLimit > 0)):
            break

        iterLimit = iterLimit - 1

    # end while

    if (iterLimit == 0):
        return 0

    uSq = cosSqAlpha * (a * a - b * b) / (b * b)
    A = 1 + uSq / 16384 * (4096 + uSq * (-768 + uSq * (320 - 175 * uSq)))
    B = uSq / 1024 * (256 + uSq * (-128 + uSq * (74 - 47 * uSq)))
    deltaSigma = B * sinSigma * (cos2SigmaM + B / 4 * (
    cosSigma * (-1 + 2 * cos2SigmaM * cos2SigmaM) - B / 6 * cos2SigmaM * (-3 + 4 * sinSigma * sinSigma) * (
    -3 + 4 * cos2SigmaM * cos2SigmaM)))

    s = b * A * (sigma - deltaSigma) * 0.000621371
    return s


def filterweather(line):
    line = line.split(',')
    try:
        mw = int(line[1])
    except:
        mw = -1
    try:
        aw1 = int(line[2])
    except:
        aw1 = -1
    try:
        aw2 = int(line[3])
    except:
        aw2 = -1

    if (mw in range(60, 70) or aw1 in range(60, 70) or aw2 in range(60, 70)):
        return True
    else:
        return False


def filtercross(line):
    taxi = line[0].split(',')
    weathertime = line[1].split(',')[0]
    taxitime = taxi[0]
    taxiinterval = getTimeInterval(taxitime)

    if weathertime == taxiinterval:
        return True
    else:
        return False

def calculateDistanceFromSubways(line):
    subwayrecord = line[0].split(',')
    joinrecord = line[1].split(',')
    droplon = joinrecord[1]
    droplat = joinrecord[2]
    # Calculating distance form subway station
    mindist = 1000
    slon, slat = subwayrecord[1].split(' ')
    d = distanceBetweenLatLong(slat, slon, droplat, droplon)
    key = joinrecord[0]+','+joinrecord[1]+','+joinrecord[2]
    value = subwayrecord[0]+','+str(d)
    return (key,value)

def getMinDist(x,y):
    if float(x.split(',')[1]) <= float(y.split(',')[1]):
        return x
    else:
        return y


def getSubways(line):
    subwayrecord = line[0].split(',')
    joinrecord = line[1].split(',')
    droplon = joinrecord[1]
    droplat = joinrecord[2]
    # Calculating distance form subway station
    mindist = 1000
    slon,slat = subwayrecord[1].split(' ')
    d = distanceBetweenLatLong(slat, slon, droplat, droplon)
    if d < 0.05:
        return True
    else:
        return False

def subwayTaxiMap(line):
    subwayrecord = line[0].split(',')
    record = line[1].split(',')
    key = subwayrecord[0]+','+record[0]+','+record[1]+','+record[2]
    value = 1

def subwayDistanceFilter(line):
    try:
        key = line[0]
        distance = float(line[1].split(',')[0])
    except:
        return False

    if distance <= 0.1 or key != "dummy":
        return True
    else:
        return False

def convertdate(line):
    taxidatetime = str(line[1])
    date_object = datetime.strptime(taxidatetime, '%m/%d/%Y %H:%M')
    newdate = date_object.strftime('%Y%m%d%H%M')
    value = str(newdate) +','+line[9]+','+line[10]
    return value

def countMap(line):
    key = line[1].split(',')[0]
    value = 1
    return (key,value)

def getNearestSubway(line):
    global subwayDict
    dict = subwayDict.value

    droplon, droplat = line.split(',')
    nearsubwaystation ="x"
    # Calculating distance form subway station
    mindist = 1000
    for key in dict:
        keystr = str(key)
        slon, slat = keystr.split(' ')
        d = distanceBetweenLatLong(slat, slon, droplat, droplon)
        if d < mindist:
            mindist = d
            nearsubwaystation = dict[key]
    key = str(nearsubwaystation)
    val = 1
    return (key, val)

def dataclean(line):
    if line[0]=='x':
        return False
    else:
        return True



# Create SparkContext
sc = SparkContext(appName="PythonCrossProduct")
# Read first file: course data
taxiweathercrossfilter = sc.textFile(sys.argv[1], 1, use_unicode=False)
# # Read second file: professor data
# weatherData = sc.textFile(sys.argv[2], 1, use_unicode=False)
subwayData = sc.textFile(sys.argv[2], 1, use_unicode=False)
dict = {}
csvfile = open('subway.csv')
csv_reader = csv.reader(csvfile)
for attribute in csv_reader:
    dict[attribute[1]] = attribute[0]

subwayDict = sc.broadcast(dict)

taxijoinsubway = taxiweathercrossfilter.map(getNearestSubway)
    # .filter(dataclean).reduceByKey(lambda x,y: x+y)
output = taxijoinsubway.collect()

for line in output:
    print('%s,%s'%(line[0],line[1]))
    # print(line)

sc.stop()
