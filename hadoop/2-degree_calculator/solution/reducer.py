#!/usr/bin/python

import sys

numNeighbors = 0
currentNode = None

for line in sys.stdin:
	line = line.strip()
	key, value = line.split('\t')
	if value == currentNode: 
		continue
	if key != currentNode:
		if currentNode is not None:
			print '%s\t%d' % (currentNode, numNeighbors)
		currentNode = key
		numNeighbors = 0
	numNeighbors += 1

if currentNode is not None:
	print '%s\t%d' % (currentNode, numNeighbors)
