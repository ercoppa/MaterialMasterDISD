local:
	cat ../datasets/mid.txt | python mapper.py | sort | python reducer.py| sort

hadoop:
	hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar  -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /wc-in -output /wc-out

input:
	hdfs dfs -rm -r -f /wc-in
	hdfs dfs -mkdir /wc-in
	hdfs dfs -copyFromLocal ../datasets/mid.txt /wc-in/

clean:
	hdfs dfs -rm -r /wc-out
	rm -rf wc-out 2>/dev/null

dump:
	hdfs dfs -copyToLocal /wc-out .
