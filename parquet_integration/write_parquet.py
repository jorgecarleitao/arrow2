import pyarrow as pa
import pyarrow.parquet
import os
import shutil

PYARROW_PATH = "fixtures/pyarrow3"
PYSPARK_PATH = "fixtures/pyspark3"

def case1(size = 1):
    int64 = [0, 1, None, 3, None, 5, 6, 7, None, 9]
    float64 = [0.0, 1.0, None, 3.0, None, 5.0, 6.0, 7.0, None, 9.0]
    string = ["Hello", None, "aa", "", None, "abc", None, None, "def", "aaa"]
    boolean = [True, None, False, False, None, True, None, None, True, True]

    return {
        "int64": int64 * size,
        "float64": float64 * size,
        "string": string * size,
        "bool": boolean * size,
        "date": int64 * size,
        "uint32": int64 * size,
    }, f"basic_nulls_{size*10}.parquet"

def write_case1_pyarrow(size = 1):
    data, path = case1(size)

    fields = [
        pa.field('int64', pa.int64()),
        pa.field('float64', pa.float64()),
        pa.field('string', pa.utf8()),
        pa.field('bool', pa.bool_()),
        pa.field('date', pa.timestamp('ms')),
        pa.field('uint32', pa.uint32()),
    ]
    schema = pa.schema(fields)

    t = pa.table(data, schema=schema)
    os.makedirs(PYARROW_PATH, exist_ok=True)
    pa.parquet.write_table(t, f"{PYARROW_PATH}/{path}")

write_case1_pyarrow(1)
write_case1_pyarrow(10)
write_case1_pyarrow(100)
write_case1_pyarrow(1000)
write_case1_pyarrow(10000)
exit(0) # we are only testing against pyarrow in the code.

def write_case1_pyspark():
    data, path = case1()
    from pyspark.sql import SparkSession

    spark = SparkSession \
        .builder \
        .getOrCreate()

    columns = list(data.keys())
    length = len(data[columns[0]])
    # transpose
    df = spark.createDataFrame([
        [int64[i], float64[i]] for i in range(length)
    ], schema=columns)

    os.makedirs(PYSPARK_PATH, exist_ok=True)
    # 1 is required here so that we convert it to a single parquet file
    df.coalesce(1).write.parquet(f"{PYSPARK_PATH}/{FILE_NAME}")

    def _single_file_spark(file_name: str):
        """
        converts a directiory with a single parquet "part" into a single parquet file.
        """
        files = os.listdir(f"{PYSPARK_PATH}/{file_name}")
        part = [a for a in files if a.endswith(".parquet")]
        assert len(path) == 1

        os.rename(f"{PYSPARK_PATH}/{file_name}/{part[0]}", f"{PYSPARK_PATH}/tmp.parquet")
        shutil.rmtree(f"{PYSPARK_PATH}/{file_name}")
        os.rename(f"{PYSPARK_PATH}/tmp.parquet", f"{PYSPARK_PATH}/{file_name}")

    _single_file_spark(FILE_NAME)
