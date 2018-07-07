include ../Makefile.inc

sample: clean
	$(PIG) -x local -p INPUT='input/cse344-test-file.gz' -p OUTPUT=output histogram.pig

chunk: clean
	$(PIG) -x local -p INPUT='input/btc-2010-chunk-000.gz' -p OUTPUT=output histogram.pig

all:	sample
	#

clean:
	-rm -rf output pig_*.log
