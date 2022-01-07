"""
Verifies that spark can correctly read a delta-encoded utf8 column written by arrow2.
"""
import os
import pyspark.sql

from main import _prepare, _expected


def test(file: str, version: str, column, compression: str, encoding: str):
    """
    Tests that pyspark can read a parquet file written by arrow2.

    In arrow2: read IPC, write parquet
    In pyarrow: read (same) IPC to Python
    In pyspark: read (written) parquet to Python
    assert that they are equal
    """
    # write parquet
    path = _prepare(file, version, compression, encoding, [column[1]])

    # read IPC to Python
    expected = _expected(file)
    expected = next(c for i, c in enumerate(expected) if i == column[1])
    expected = expected.combine_chunks().tolist()

    # read parquet to Python
    spark = pyspark.sql.SparkSession.builder.config(
        # see https://stackoverflow.com/a/62024670/931303
        "spark.sql.parquet.enableVectorizedReader",
        "false",
    ).getOrCreate()

    result = spark.read.parquet(path).select(column[0]).collect()
    result = [r[column[0]] for r in result]
    os.remove(path)

    # assert equality
    assert expected == result


test("generated_primitive", "2", ("utf8_nullable", 24), "uncompressed", "delta")
test("generated_primitive", "2", ("utf8_nullable", 24), "snappy", "delta")

test("generated_dictionary", "1", ("dict0", 0), "uncompressed", "")
test("generated_dictionary", "1", ("dict0", 0), "snappy", "")
test("generated_dictionary", "2", ("dict0", 0), "uncompressed", "")
test("generated_dictionary", "2", ("dict0", 0), "snappy", "")

test("generated_dictionary", "1", ("dict1", 1), "uncompressed", "")
test("generated_dictionary", "1", ("dict1", 1), "snappy", "")
test("generated_dictionary", "2", ("dict1", 1), "uncompressed", "")
test("generated_dictionary", "2", ("dict1", 1), "snappy", "")

test("generated_dictionary", "1", ("dict2", 2), "uncompressed", "")
test("generated_dictionary", "1", ("dict2", 2), "snappy", "")
test("generated_dictionary", "2", ("dict2", 2), "uncompressed", "")
test("generated_dictionary", "2", ("dict2", 2), "snappy", "")
