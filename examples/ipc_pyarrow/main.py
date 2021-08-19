import pyarrow as pa
from time import sleep


data = [
    pa.array([1, 2, 3, 4]),
    pa.array(["foo", "bar", "baz", None]),
    pa.array([True, None, False, True]),
]

batch = pa.record_batch(data, names=["f0", "f1", "f2"])
writer = pa.ipc.new_stream("data.arrows", batch.schema)
while True:
    for _ in range(10):
        writer.write(batch)
    sleep(1)
