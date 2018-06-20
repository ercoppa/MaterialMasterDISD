#!/usr/bin/python

import sys
import string

for line in sys.stdin:
    line = line.strip()
    words = line.split()
    if len(line) > 1:
        for word in words:
            word = word.lower()
            word = word.strip(string.punctuation)
            if len(word) > 0:
                print '%s %d' % (word, 1)
