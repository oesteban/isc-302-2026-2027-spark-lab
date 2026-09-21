IMAGE ?= spark:3.5.4-python3
DOCKER = docker run --rm -v "$(PWD):/work:ro" -w /work $(IMAGE) /opt/spark/bin/spark-submit
RUN    = $(DOCKER) --master 'local[*]'
RUN1   = $(DOCKER) --master 'local[1]'

.PHONY: check df rdd slow slow1 clean

## check: run both counts and assert they agree
check:
	@echo "== DataFrame =="
	@$(RUN) wordcount_df.py  2>/dev/null | sed -n '/now an action/,/^input partitions/p' | grep -E '^ +[0-9]+ ' > /tmp/302-df.txt
	@echo "== RDD =="
	@$(RUN) wordcount_rdd.py 2>/dev/null | sed -n '/now an action/,/^input partitions/p' | grep -E '^ +[0-9]+ ' > /tmp/302-rdd.txt
	@if diff -q /tmp/302-df.txt /tmp/302-rdd.txt >/dev/null; then \
	  echo; echo "OK: both APIs agree on the top 15"; cat /tmp/302-df.txt; \
	else \
	  echo; echo "MISMATCH:"; diff /tmp/302-df.txt /tmp/302-rdd.txt; exit 1; \
	fi

df:   ; $(RUN) wordcount_df.py
rdd:  ; $(RUN) wordcount_rdd.py
slow:  ; $(RUN) slowdown.py
slow1: ; $(RUN1) slowdown.py

clean: ; @rm -f /tmp/302-df.txt /tmp/302-rdd.txt
