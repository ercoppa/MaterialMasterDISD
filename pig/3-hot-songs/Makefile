include ../Makefile.inc

all:	clean
	$(PIG) -p INPUT=input -p OUTPUT=output hot-songs.pig

clean:
	@rm -rf output 2>/dev/null || true
	@-rm pig_*.log 2>/dev/null || true

interactive:
	$(PIG)
