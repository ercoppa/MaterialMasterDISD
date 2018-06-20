#!/usr/bin/python

import sys

current = 0
lastKey = None

for line in sys.stdin:
    line = line.strip()
    key, value = line.split(' ')
    try:
        value = value
        if value == lastKey: # skip self edges
            continue
        if key != lastKey:
            if lastKey is not None:
                print '%s %d' % (lastKey, current)
            lastKey = key
            current = 0
        current += 1
    except ValueError:
        pass

if lastKey is not None:
    print '%s %d' % (lastKey, current)
