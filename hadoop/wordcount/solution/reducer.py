#!/usr/bin/python

import sys

lastkey = None
current = 0

for line in sys.stdin:
    line = line.strip()
    key, value = line.split(' ')
    try:
        value = int(value)
        if key != lastkey:
            if lastkey is not None:
                print '%s %d' % (lastkey, current)
            lastkey = key
            current = 0
        current += 1
    except ValueError:
        pass

if lastkey is not None:
    print '%s %d' % (lastkey,current)
