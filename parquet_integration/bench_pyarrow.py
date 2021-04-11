import timeit

import pyarrow.parquet

def f(column):
    pyarrow.parquet.read_table("fixtures/pyarrow3/basic_nullable_100000.parquet", columns=[column])

seconds = timeit.Timer(lambda: f("int64")).timeit(number=512) / 512
microseconds = seconds * 1000 * 1000
print("read u64 100000     time: {:.2f} us".format(microseconds))

seconds = timeit.Timer(lambda: f("string")).timeit(number=512) / 512
microseconds = seconds * 1000 * 1000
print("read utf8 100000    time: {:.2f} us".format(microseconds))
