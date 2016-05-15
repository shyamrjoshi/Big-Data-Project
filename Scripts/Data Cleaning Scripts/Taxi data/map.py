#!/usr/bin/env python

import sys
from datetime import datetime


for line in sys.stdin:
    line = line.strip()
    cols = line.split(',')
    dt = cols[1]

    try:
        date_object = datetime.strptime(dt, '%Y-%m-%d %H:%M:%S')
        newdate = date_object.strftime('%Y%m%d%H%M')
    except ValueError:
        newdate = '000000000000'

    values = (',').join(cols[3:])
    print('%s\t%s'%(str(newdate),values))



