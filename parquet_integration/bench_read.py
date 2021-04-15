import timeit
import io

import pyarrow.parquet


def bench(log2_size: int, datatype: str):
    with open(f"fixtures/pyarrow3/v1/benches_{2**log2_size}.parquet", "rb") as f:
        data = f.read()
    data = io.BytesIO(data)

    def f():
        pyarrow.parquet.read_table(data, columns=[datatype])

    seconds = timeit.Timer(f).timeit(number=512) / 512
    microseconds = seconds * 1000 * 1000
    print(f"read {datatype} 2^{log2_size}     time: {microseconds:.2f} us")

#for i in range(10, 22, 2):
#    bench(i, "int64")

for i in range(10, 22, 2):
    bench(i, "string")

for i in range(10, 22, 2):
    bench(i, "bool")
