include ../Makefile.inc

all:	clean
	$(PIG) -p OUTPUT=output top-pages.pig

clean:
	@rm -rf output 2>/dev/null || true
	@rm pig_*.log 2>/dev/null || true
