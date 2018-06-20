local:
	cat datasets/test.dat | python mapper.py | sort | python reducer.py| sort

hadoop:
	hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-2.7.2.jar  -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /p2-in -output /p2-out

input:
	hdfs dfs -rm -r /p2-in
	hdfs dfs -mkdir /p2-in
	hdfs dfs -copyFromLocal datasets/facebook.txt /p2-in/

clean:
	hdfs dfs -rm -r /p2-out
	rm -rf p2-out 2>/dev/null

dump:
	hdfs dfs -copyToLocal /p2-out .
