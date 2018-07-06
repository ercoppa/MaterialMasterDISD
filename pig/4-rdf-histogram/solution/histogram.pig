REGISTER lib/myudfs.jar;

-- load the test file into Pig
raw = LOAD '$INPUT' USING TextLoader as (line:chararray);

-- parse each line into ntriples
ntriples = foreach raw generate FLATTEN(myudfs.RDFSplit3(line)) as (subject:chararray,predicate:chararray,object:chararray);

--group the n-triples by subject
subjects = group ntriples by (subject) PARALLEL 50;

count1 = FOREACH subjects GENERATE COUNT($1);
count2 = group count1 by $0;

pairs = FOREACH count2 GENERATE $0, COUNT($1);

store pairs into '$OUTPUT' using PigStorage();
