## Useful Hadoop commands:


*  To start HDFS and YARN: 
```
> start-all.sh
```
Or, to start them separately:
```
> start-dfs.sh
> start-yarn.sh
```
Try use jps to see if everything is fine. You should get something like:
```
> jps
<PID_NUMBER> NodeManager
<PID_NUMBER> SecondaryNameNode
<PID_NUMBER> DataNode
<PID_NUMBER> ResourceManager
<PID_NUMBER> Jps
<PID_NUMBER> NameNode
```



* To create a directory on HDFS:
```
> hdfs dfs -mkdir /wc-in
```
where `/wc-in` is the path on HDFS where we want to create a directory.

* To delete a directory on HDFS:
```
> hdfs dfs -rm -r -f /wc-out
```
where `/wc-out` is the path on the HDFS of the file/directory that we want to delete.

* To list the contents of a directory on the HDFS:
```
> hdfs dfs -ls /wc-in
```
where `/wc-in` is the path on HDFS that we want to list.

* To load a file on HDFS:
```
> hdfs dfs -copyFromLocal datasets/mid.txt /pathToMyDirectory
```
where `datasets/mid.txt` is path to a file on the local filesystem and `/wc-in` is directory on the HDFS where we want to store the file.

* To run mapper/reducer written in Python on Hadoop:
```
hadoop jar ${HADOOP_HOME}/share/hadoop/tools/lib/hadoop-streaming-*.jar  -files mapper.py,reducer.py -mapper mapper.py -reducer reducer.py -input /wc-in -output /wc-out
```
where:
 *  `mapper.py` and `reducer.py` are the Python implementations of the mapper and reducer functions stored in the current directory
 * `/wc-in` is the input directory for the Hadoop job
 * `/wc-out` is the output directory for the Hadoop job

* To read a file from the HDFS:
```
> hdfs dfs -cat /wc-out/part-r-00000 | less
```
where `/wc-out/part-r-00000` is the file on the HDFS that we want to read.

* To export a file from the HDFS to the local filesystem:
```
> hdfs dfs -copyToLocal /wc-out .
```
where `/wc-out` is the directory on the HDFS that we want to export and `.` is the path of the local filesystem where we want to store the data.

*  To stop HDFS and YARN: 
```
> stop-all.sh
```
Or, to stop them separately:
```
> stop-dfs.sh
> stop-yarn.sh
```