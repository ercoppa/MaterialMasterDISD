local:
	cat ../datasets/facebook.txt | python mapper.py | sort -n | python reducer.py| sort

hadoop:
	hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar  -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /dc-in -output /dc-out

input:
	hdfs dfs -rm -r -f /dc-in
	hdfs dfs -mkdir /dc-in
	hdfs dfs -copyFromLocal ../datasets/facebook.txt /dc-in/

clean:
	hdfs dfs -rm -r /dc-out
	rm -rf dc-out 2>/dev/null

dump:
	hdfs dfs -copyToLocal /dc-out .
