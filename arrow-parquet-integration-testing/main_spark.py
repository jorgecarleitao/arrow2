"""
Verifies that spark can correctly read a delta-encoded utf8 column written by arrow2.
"""
import os
import pyspark.sql

from main import _prepare, _expected

_file = "generated_primitive"
_version = "2"
_encoding = "delta"
column = ("utf8_nullable", 24)

expected = _expected(_file)
expected = next(c for i, c in enumerate(expected) if i == column[1])
expected = expected.combine_chunks().tolist()

path = _prepare(_file, _version, _encoding, [column[1]])

spark = pyspark.sql.SparkSession.builder.config(
    # see https://stackoverflow.com/a/62024670/931303
    "spark.sql.parquet.enableVectorizedReader",
    "false",
).getOrCreate()

df = spark.read.parquet(path)

r = df.select(column[0]).collect()
os.remove(path)

result = [r[column[0]] for r in r]
assert expected == result
