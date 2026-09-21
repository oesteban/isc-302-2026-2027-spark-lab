#!/usr/bin/env python
"""Make the same job slower on purpose, and watch where the time goes.

    spark-submit --master "local[1]" slowdown.py
    spark-submit --master "local[*]" slowdown.py

Same data, same answer, four wall clocks. A partition is a unit of work that
has to be scheduled, shipped and collected. On four megabytes, four hundred of
them cost more to administer than they save by being parallel.
"""
import sys
import time
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

DATA = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("data")
PARTITIONS = (8, 800, 8000)

spark = SparkSession.builder.appName("slowdown").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

master = spark.sparkContext.master
cores = spark.sparkContext.defaultParallelism
print(f"\nmaster={master}   cores Spark will use={cores}")

lines = spark.read.text(str(DATA / "20news" / "*.txt"))
print(f"partitions Spark chose for the input: {lines.rdd.getNumPartitions()}")

words = lines.select(
    F.explode(F.split(F.lower(F.col("value")), r"\W+")).alias("word")
).filter(F.col("word") != "")

print(f"\n{'partitions':>12}  {'seconds':>8}  {'distinct words':>15}")
for n in PARTITIONS:
    t0 = time.perf_counter()
    got = words.repartition(n).groupBy("word").count().count()
    print(f"{n:>12}  {time.perf_counter() - t0:>8.1f}  {got:>15}")

spark.stop()
