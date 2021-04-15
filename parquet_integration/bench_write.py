"""
Benchmark of writing a pyarrow table of size N to parquet.
"""
import io
import os
import timeit

import numpy
import pyarrow.parquet


def case_basic_nullable(size = 1):
    int64 = [0, 1, None, 3, None, 5, 6, 7, None, 9]
    float64 = [0.0, 1.0, None, 3.0, None, 5.0, 6.0, 7.0, None, 9.0]
    string = ["Hello", None, "aa", "", None, "abc", None, None, "def", "aaa"]
    boolean = [True, None, False, False, None, True, None, None, True, True]

    fields = [
        pa.field('int64', pa.int64()),
        pa.field('float64', pa.float64()),
        pa.field('string', pa.utf8()),
        pa.field('bool', pa.bool_()),
        pa.field('date', pa.timestamp('ms')),
        pa.field('uint32', pa.uint32()),
    ]
    schema = pa.schema(fields)

    return {
        "int64": int64 * size,
        "float64": float64 * size,
        "string": string * size,
        "bool": boolean * size,
        "date": int64 * size,
        "uint32": int64 * size,
    }, schema, f"basic_nullable_{size*10}.parquet"

def bench(log2_size: int, datatype: str):

    if datatype == 'int64':
        data = [0, 1, None, 3, 4, 5, 6, 7] * 128   # 1024 entries
        field = pyarrow.field('int64', pyarrow.int64())
    elif datatype == 'utf8':
        # 4 each because our own benches also use 4
        data = ["aaaa", "aaab", None, "aaac", "aaad", "aaae", "aaaf", "aaag"] * 128   # 1024 entries
        field = pyarrow.field('utf8', pyarrow.utf8())
    elif datatype == 'bool':
        data = [True, False, None, True, False, True, True, True] * 128   # 1024 entries
        field = pyarrow.field('bool', pyarrow.bool_())

    data = data * 2**log2_size

    t = pyarrow.table([data], schema=pyarrow.schema([field]))

    def f():
        pyarrow.parquet.write_table(t,
            io.BytesIO(),
            use_dictionary=False,
            compression=None,
            write_statistics=False,
            data_page_size=2**40,  # i.e. a large number to ensure a single page
            data_page_version="1.0")

    seconds = timeit.Timer(f).timeit(number=512) / 512
    microseconds = seconds * 1000 * 1000
    print(f"write {datatype} 2^{10 + log2_size}     time: {microseconds:.2f} us")

for i in range(0, 12, 2):
    bench(i, "int64")

for i in range(0, 12, 2):
    bench(i, "utf8")

for i in range(0, 12, 2):
    bench(i, "bool")
