#!/usr/bin/env python

import sys
from datetime import datetime


for line in sys.stdin:
    line = line.strip()
    key,value = line.split('\t')

    if key == '000000000000':
        continue
    print('%s,%s'%(key,value))


