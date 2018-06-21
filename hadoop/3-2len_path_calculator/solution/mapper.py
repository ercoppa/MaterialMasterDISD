#!/usr/bin/python

import sys		# sys.stdin

for line in sys.stdin:
    edge = line.rstrip("\n").strip()
    if "\t" in edge:
        nodes = edge.split("\t")
    else:
        nodes = edge.split(":")
    if len(nodes) != 2:
        #print "Invalid edge: " + edge
        continue
    print '%s %s' % (nodes[0], nodes[1])
