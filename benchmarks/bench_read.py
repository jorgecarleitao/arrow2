import timeit
import io
import os
import json

import pyarrow.parquet


def _bench_single(log2_size: int, column: str, use_dict: bool) -> float:
    if use_dict:
        path = f"fixtures/pyarrow3/v1/dict/benches_{2**log2_size}.parquet"
    else:
        path = f"fixtures/pyarrow3/v1/benches_{2**log2_size}.parquet"
    with open(path, "rb") as f:
        data = f.read()
    data = io.BytesIO(data)

    def f():
        pyarrow.parquet.read_table(data, columns=[column])

    seconds = timeit.Timer(f).timeit(number=512) / 512
    us = seconds * 1000 * 1000 * 1000
    return us


def _report(name: str, result: float):
    path = f"benchmarks/runs/{name}/new"
    os.makedirs(path, exist_ok=True)
    with open(f"{path}/estimates.json", "w") as f:
        json.dump({"mean": {"point_estimate": result}}, f)


def _bench(size, ty):
    column, use_dict = {
        "i64": ("int64", False),
        "bool": ("bool", False),
        "utf8": ("string", False),
        "utf8 dict": ("string", True),
    }[ty]

    result = _bench_single(size, column, use_dict)
    print(result)
    _report(f"read {ty} 2_{size}", result)


for size in range(10, 22, 2):
    for ty in ["i64", "bool", "utf8", "utf8 dict"]:
        print(size, ty)
        _bench(size, ty)
