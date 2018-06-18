local:
	cat datasets/facebook.txt | python mapper.py | sort | python reducer.py| sort

hadoop:
	hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /wc-in -output /wc-out

input:
	hdfs dfs -rm -r /dc-in
	hdfs dfs -mkdir /dc-in
	hdfs dfs -copyFromLocal datasets/facebook.txt /dc-in/

clean:
	hdfs dfs -rm -r /dc-out
	rm -rf dc-out 2>/dev/null

dump:
	hdfs dfs -copyToLocal /dc-out .
