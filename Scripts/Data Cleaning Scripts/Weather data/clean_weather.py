import sys
from datetime import datetime
from datetime import timedelta
from pytz import timezone

weatherfile = open('weather-data.txt')
outputfile = open('latestweather.txt','w')
for line in weatherfile:

    try:
        line = line.strip().split(' ')
        currentdatetime = line[2]
        year = int(currentdatetime[:4])
        month= int(currentdatetime[4]+currentdatetime[5])
        date = int(currentdatetime[6]+currentdatetime[7])
        hours = int(currentdatetime[8]+currentdatetime[9])
        mins = int(currentdatetime[10]+currentdatetime[11])
        date_object = datetime.strptime(currentdatetime, '%Y%m%d%H%M%S')
        date_object = date_object.replace(hour=hours,minute=mins,year=year,day=date,month=month)
        date_object = date_object - timedelta(hours=4)
        line[2] = date_object.strftime('%Y%m%d%H%M')
        line = (' ').join(line)
        outputfile.write(line+'\n')
    except:
        continue

