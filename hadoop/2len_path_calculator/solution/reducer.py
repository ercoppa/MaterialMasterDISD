#!/usr/bin/python

import sys

neighbors = set()
lastKey = None
total_count = 0

def compute_len2(node, neighbors):
    count = 0
    neighbors = list(neighbors)
    for u in range(len(neighbors)):
        for w in range(u, len(neighbors)):
            if neighbors[u] != neighbors[w]:
                #print "Path: (%s %s %s)" % (neighbors[u], node, neighbors[w])
                count += 1

    global total_count
    total_count += count
    return count

for line in sys.stdin:
    line = line.strip()
    key, value = line.split(' ')
    try:
        if value == lastKey: # skip self edges
            continue

        if key != lastKey:
            if lastKey is not None:
                n = compute_len2(lastKey, neighbors)
                #print '%s %d' % (lastKey, n)
            lastKey = key
            neighbors = set()

        neighbors.add(value)

    except ValueError:
        pass

if lastKey is not None:
    n = compute_len2(lastKey, neighbors)
    #print '%s %d' % (lastKey, n)

print total_count
