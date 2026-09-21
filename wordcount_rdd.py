#!/usr/bin/env python
"""The same count, with the RDD API.

    spark-submit wordcount_rdd.py

Same answer, different machinery. An RDD has no plan to print, because there
is no optimiser to print one: `toDebugString()` shows the lineage you built
by hand. Compare the two, and compare the partition counts.
"""
import re
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession

DATA = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
TOP_N = 15
SPLIT = re.compile(r"\W+")

stopwords = {
    line.strip().lower()
    for line in (DATA / "stopwords.txt").read_text().splitlines()
    if line.strip()
}

spark = SparkSession.builder.appName("wordcount-rdd").getOrCreate()
sc = spark.sparkContext
sc.setLogLevel("WARN")

bc_stop = sc.broadcast(stopwords)

lines = sc.textFile(str(DATA / "20news" / "*.txt"))

counts = (
    lines.flatMap(lambda line: SPLIT.split(line.lower()))
         .filter(lambda w: len(w) >= 3 and not w.isdigit() and w not in bc_stop.value)
         .map(lambda w: (w, 1))
         .reduceByKey(lambda a, b: a + b)
)

print("\n=== the lineage you built by hand ===")
print(counts.toDebugString().decode())

print("\n=== now an action ===")
t0 = time.perf_counter()
top = counts.takeOrdered(TOP_N, key=lambda kv: -kv[1])
elapsed = time.perf_counter() - t0

for word, n in top:
    print(f"{n:>8}  {word}")
print(f"\ninput partitions : {lines.getNumPartitions()}")
print(f"elapsed          : {elapsed:.1f} s")

spark.stop()
