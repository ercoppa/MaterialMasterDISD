SONGS = LOAD '$INPUT' USING PigStorage(',') AS (artist_name : chararray, duration : float, song_hotness : float, title : chararray, year : chararray);
FILTERED_SONGS = FILTER SONGS BY year IS NOT NULL AND year != '0' AND song_hotness IS NOT NULL AND song_hotness > 0.0;
DS = FOREACH FILTERED_SONGS GENERATE artist_name, title, year, song_hotness ,SUBSTRING (year, 0, 3) AS decade;
GS = GROUP DS BY decade;
TOP = FOREACH GS { 
	OS = ORDER DS BY song_hotness DESC; 
	TS = LIMIT OS 1; 
	GENERATE group as decade, FLATTEN (TS); 
};
OTS = ORDER TOP BY year ASC;
RES = FOREACH OTS GENERATE decade, artist_name, title, year, song_hotness;
STORE RES INTO '$OUTPUT';
