local:
	cat ../datasets/test.dat | python mapper.py | sort -n | python reducer.py| sort -n

hadoop:
	hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar  -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /p2-in -output /p2-out

input:
	hdfs dfs -rm -r -f /p2-in
	hdfs dfs -mkdir /p2-in
	hdfs dfs -copyFromLocal ../datasets/facebook.txt /p2-in/

clean:
	hdfs dfs -rm -r /p2-out
	rm -rf p2-out 2>/dev/null

dump:
	hdfs dfs -copyToLocal /p2-out .
