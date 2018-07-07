L = LOAD '$INPUT' AS ( line : chararray );
W = FOREACH L GENERATE FLATTEN ( TOKENIZE ( line )) AS word;
GW = GROUP W BY word;
CW = FOREACH GW GENERATE group as word, COUNT (W) as count;
SW = ORDER CW BY count DESC;
TOP = LIMIT SW 10;
STORE TOP INTO '$OUTPUT';
