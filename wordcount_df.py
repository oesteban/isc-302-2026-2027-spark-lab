#!/usr/bin/env python
"""Count the words in the 20 Newsgroups sample, with the DataFrame API.

Run it inside the Spark image:

    spark-submit wordcount_df.py

The point of this script is not the word list. It is the plan printed above
the word list: `explain()` runs before any data is read, because none of the
lines before it did any work.
"""
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
TOP_N = 15

stopwords = {
    line.strip().lower()
    for line in (DATA / "stopwords.txt").read_text().splitlines()
    if line.strip()
}

spark = SparkSession.builder.appName("wordcount-df").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 1. Read every message as rows of one column, "value".
lines = spark.read.text(str(DATA / "20news" / "*.txt"))

# 2. Lowercase, split on anything that is not a word character, one row per token.
#    Note the single backslash: r"\W+" is the regex. r"\\W+" would match a
#    literal backslash, which is a mistake that produces one token per line.
words = lines.select(
    F.explode(F.split(F.lower(F.col("value")), r"\W+")).alias("word")
)

# 3. Drop short tokens, pure numbers and stopwords. The length filter is not
#    cosmetic: r"\W+" splits "don't" into "don" and "t", so without it the
#    top of the list fills with the debris of apostrophes.
words = words.filter(
    (F.length(F.col("word")) >= 3)
    & (~F.col("word").rlike(r"^[0-9]+$"))
    & (~F.col("word").isin(list(stopwords)))
)

# 4. Count, then order. Both are transformations: still nothing has run.
counts = words.groupBy("word").count().orderBy(F.desc("count"))

print("\n=== the plan, before a single byte has been read ===")
counts.explain()

print("\n=== now an action, and only now does anything run ===")
t0 = time.perf_counter()
rows = counts.take(TOP_N)
elapsed = time.perf_counter() - t0

for r in rows:
    print(f"{r['count']:>8}  {r['word']}")
print(f"\ninput partitions : {lines.rdd.getNumPartitions()}")
print(f"elapsed          : {elapsed:.1f} s")

spark.stop()
