#!/bin/bash

hdfs dfs -rm -r -f /wc-output
yarn jar ~/Desktop/WordCount-solution.jar -r 1 /wc-in /wc-output

exit 0

